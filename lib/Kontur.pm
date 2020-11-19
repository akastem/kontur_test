package Kontur;
use strict;
use warnings;
use 5.010;
use POSIX;
use AnyEvent;
use FindBin qw<$Bin>;
use Plack::Request;
use Plack::Response;
use DBI;
use DBD::Pg qw(:async);
use JSON::XS;
use Carp ();
use Digest::MD5 ();
use Redis;
use DDP;

sub new {
    my $class = shift;
    my $self  = {@_};

    bless $self, $class;

    $self->init;
    $self;
}

sub init {
    my $self = shift;
    my $logs = "$Bin/../logs";

    open $self->{"LOG"}, ">>", "$logs/kontur.log" or die "Could not open LOG file";

    $self->log("INIT");

    my $connect_organizations = $self->connect_organizations;
    my $connect_invoices = $self->connect_invoices;
    my $connect_redis = $self->connect_redis;

    $self->_croak("No connection to organizations") unless ($connect_organizations && %$connect_organizations);
    $self->_croak("No connection to invoices") unless ($connect_invoices && %$connect_invoices);

    # Connect organizations
    #
    my $db_organizations = DBI->connect(
        "dbi:Pg:dbname=${$connect_organizations}{dbname};host=${$connect_organizations}{host};port=${$connect_organizations}{port}",
        $connect_invoices->{"user"},
        $connect_invoices->{"password"},
        {
            "AutoCommit"        => 0,
            "RaiseError"        => 1,
            "pg_enable_utf8"    => 1,
            "pg_server_prepare" => 0,
            "quote_char"        => '"',
            "name_sep"          => "."
        }
    ) or $self->_croak(DBI->errstr);

    $db_organizations->trace(1, "$logs/trace_organizations.log");

    $self->{"db_organizations"} = $db_organizations;

    # Connect invoices
    #
    my $db_invoices = DBI->connect(
        "dbi:Pg:dbname=${$connect_invoices}{dbname};host=${$connect_invoices}{host};port=${$connect_invoices}{port}",
        $connect_invoices->{"user"},
        $connect_invoices->{"password"},
        {
            "AutoCommit"        => 0,
            "RaiseError"        => 1,
            "pg_enable_utf8"    => 1,
            "pg_server_prepare" => 0,
            "quote_char"        => '"',
            "name_sep"          => "."
        }
    ) or $self->_croak(DBI->errstr);

    $db_invoices->trace(1, "$logs/trace_invoices.log");

    $self->{"db_invoices"} = $db_invoices;

    # Connect Redis
    #
    if ($connect_redis) {
        my $redis = Redis->new("server" => "${$connect_redis}{host}:${$connect_redis}{port}");
        $self->{"db_redis"} = $redis if $redis->ping;
    }

    # Если установлен dblink, можно попробовать запросы через линк
    # TODO: dblink(text connstr, text sql [, bool fail_on_error]) returns setof record
    #
    $self->sql_check_dblink;

    # Способ выборки
    #
    $self->{"way"} //= "inn";

    $self->{"letters"} = ['a'..'z'];
}

BEGIN {
    no strict "refs";
    for my $method (qw<db_organizations db_invoices db_redis params way connect_organizations connect_invoices connect_redis>) {
        *$method = sub { @_ > 1 ? $_[0]->{$method} = $_[1] : $_[0]->{$method} }
    }
}

sub startup {
    my ($self, $env) = @_;

    $self->{"env"} = $env;

    if ($env->{"psgi.streaming"} && $env->{"psgi.nonblocking"}) {
        $self->log("Mode: async");
        return sub {
            my $respond = shift;
            $env->{"timer"} = AnyEvent->timer(
                "after" => 0.1,
                "cb" => sub {
                    $respond->($self->request);
                }
            );
        }
    }
    elsif ($env->{"psgi.streaming"}) {
        $self->log("Mode: prefork");
        return sub {
            my $respond = shift;
            $respond->($self->request);
        };
    }
    else {
        $self->log("Mode: sync");
        $self->request;
    }
}

sub request {
    my $self = shift;

    # Параметры
    #
    my $req = Plack::Request->new($self->{"env"});

    my $content = $req->content;

    my $query_parameters   = $req->query_parameters->as_hashref;
    my $body_parameters    = $req->body_parameters->as_hashref;
    my $content_parameters = $content ? decode_json($content) : {};

    $self->{"params"} = %$query_parameters   ? $query_parameters   :
                        %$body_parameters    ? $body_parameters    : 
                        %$content_parameters ? $content_parameters : {};

    my $line_params = join ';' => map { $self->{"params"}->{$_} } sort keys %{$self->{"params"}};
    $self->{"code"} = Digest::MD5::md5_hex($line_params);
    $self->{"way"}  = delete $self->{"params"}->{"way"} if $self->{"params"}->{"way"};

    $self->_croak(sprintf "Invalid search method: %s", $self->{"way"})
        unless $self->{"way"} =~ /^(?:in|copy)$/;

    $self->main;
}

sub main {
    my $self = shift;

    my $csv; $csv = $self->db_redis->get($self->{"code"})
        if $self->db_redis;

    unless ($csv) {
        my $method_search = "search_" . $self->way;
        my $invoices = $self->$method_search;

        $csv  = "period;owner_inn;type;contractor_inn;date;number;json\n";
        $csv .= (join ";" => @$_) . "\n" foreach @$invoices;

        if ($self->db_redis && $csv) {
            $self->db_redis->setnx($self->{"code"}, $csv);
            $self->db_redis->expire($self->{"code"}, $self->{"connect_redis"}->{"EX"});
        }
    }

    $self->response($csv);
}

sub search_in {
    my $self = shift;
    my $params = $self->params;

    my $organizations = [];
    my $invoices = [];

    if (my $name = delete $params->{"name"}) {
        $organizations = $self->db_organizations->selectall_arrayref(
                "SELECT inn FROM kontur.organizations WHERE name LIKE ?",
                {}, $name . "%"
            );

        return $invoices unless @$organizations;
    }

    my ($where, @owner_inns, @contractor_inns);

    if (@$organizations) {
        @owner_inns = map {$_->[0]} @$organizations;
        @contractor_inns = @owner_inns;

        push(@owner_inns, delete $params->{"owner_inn"})
            if $params->{"owner_inn"};
        push(@contractor_inns, delete $params->{"contractor_inn"})
            if $params->{"contractor_inn"};

        if (@owner_inns) {
            $where =
                "(owner_inn IN (" . (join ',' => map { "'$_'" } (@owner_inns)) . ") OR " .
                "contractor_inn IN (" . (join ',' => map { "'$_'" } (@contractor_inns)) . "))";
        }
    }

    $where .= ($where && %$params ? " AND " : "") . (join ' AND ' => map { "$_=?" if $_ } sort keys %$params);

    $invoices = $self->db_invoices->selectall_arrayref(
            "SELECT * FROM kontur.invoices WHERE " . $where,
            {}, (map { $$params{$_} } sort keys %$params)
        ) if $where;

    $invoices;
}

sub search_copy {
    my $self = shift;
    my $params = $self->params;
    my $invoices = [];
    my ($where, $tmp_table);

    my $connect_organizations = $self->connect_organizations;
    my $connect_invoices = $self->connect_invoices;

    if (my $name = delete $params->{"name"}) {
        $tmp_table = "tmp_"; map { $tmp_table .= $self->{"letters"}->[int(rand(26))] } (1..8);

        $self->{"tmp_tables"}->{$tmp_table} = 1;

        $self->db_invoices->do("CREATE TABLE IF NOT EXISTS kontur.$tmp_table (inn text NOT NULL, PRIMARY KEY (inn))");

        my $res = $self->db_invoices->commit;

        unless ($res) {
            $self->log("Failed to create a temporary table");
            return $self->search_in;
        }

        my $cmd_copy = sprintf(qq<PGPASSWORD=%s psql -h %s -p %s -U %s %s -c "copy (SELECT inn FROM kontur.organizations WHERE name LIKE '%s%%'>,
                (map { $connect_organizations->{"$_"} } (qw<password host port user dbname>)), $name
            );

        $cmd_copy .= sprintf " OR inn='%s'", delete $params->{"owner_inn"}
            if $params->{"owner_inn"};

        $cmd_copy .= sprintf " OR inn='%s'", delete $params->{"contractor_inn"}
            if $params->{"contractor_inn"};

        $cmd_copy .= ') to stdout"';

        $cmd_copy .= " | " . sprintf(qq<PGPASSWORD=%s psql -h %s -p %s -U %s %s -c "copy kontur.%s from stdin">,
                (map { $connect_invoices->{"$_"} } (qw<password host port user dbname>)), $tmp_table
            );

        $res = `$cmd_copy`;

        if ($res =~ /^COPY\s(\d+)/) {
            return $invoices unless $1;
        }
        else {
            $self->log("Could not copy");
            return $self->search_in;
        }

        $where =
                "(owner_inn IN (SELECT inn FROM kontur.$tmp_table) OR " .
                "contractor_inn IN (SELECT inn FROM kontur.$tmp_table))";
    }

    $where .= ($where && %$params ? " AND " : "") . (join ' AND ' => map { "$_=?" if $_ } sort keys %$params);

    $invoices = $self->db_invoices->selectall_arrayref(
            "SELECT * FROM kontur.invoices WHERE " . $where,
            {}, (map { $$params{$_} } sort keys %$params)
    ) if $where;

    if ($tmp_table) {
        $self->db_invoices->do("DROP TABLE IF EXISTS kontur.$tmp_table");
        my $res = $self->db_invoices->commit;
        delete $self->{"tmp_tables"}->{$tmp_table} if $res;
    }

    $invoices;
}

sub response {
    my ($self, $csv) = @_;

    my $res = Plack::Response->new(200);
    my $headers = $res->headers;

    $res->headers(["Content-Type" => "application/octet-stream"]);
    $res->headers(["Content-Disposition" => qq<attachment; filename="${$self}{code}.csv">]);
    $res->body($csv);

    return $res->finalize;
}

sub sql_check_dblink {
    my $self = shift;

    my $sql = q<SELECT pg_namespace.nspname, pg_proc.proname
        FROM pg_proc, pg_namespace 
        WHERE pg_proc.pronamespace=pg_namespace.oid AND pg_proc.proname LIKE '%dblink%'>;

    $self->{"dblink"} = $self->db_invoices->selectrow_arrayref($sql);
}

sub _croak {
    my ($self, $msg) = @_;

    $self->log($msg);

    Carp::croak($msg);
}

sub log {
    my ($self, $line) = @_;

    my $ctime = strftime "%Y-%m-%d %H:%M:%S", localtime time;

    $line = sprintf "[%s][%s:%s] %s\n", $ctime, (caller)[1,2], $line;

    print $line if $self->{"debug"};

    syswrite $self->{"LOG"}, $line;
}

sub DESTROY { 
    my $self = shift;

    $self->log("DESTROY");

    foreach my $tmp_table (keys %{$self->{"tmp_tables"}}) {
        $self->db_invoices->do("DROP TABLE IF EXISTS kontur.$tmp_table");
        $self->db_invoices->commit;
    }

    $self->db_organizations->disconect();
    $self->db_invoices->disconect();

    close $self->{"LOG"};

    undef $self;
}

1;