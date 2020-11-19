#!/usr/bin/perl
use strict;
use warnings;
use DBI;
use DBD::Pg;
use DDP;
use AnyEvent;

my $dbname_organizations = "kontur_organizations";
my $dbname_invoices = "kontur_invoices";

my $host = "localhost";
my $port = 5432;
my $username = "postgres";
my $password = "Kf8idXA9bqlA";

my @letters = ('a'..'z');
my @numbers = (0..9);
my @characters = (@letters, @numbers);

my $letters_count = scalar @letters;
my $numbers_count = scalar @numbers;
my $characters_count = scalar @characters;

my $db_organizations = DBI->connect("dbi:Pg:dbname=$dbname_organizations;host=$host;port=$port",
        $username,
        $password,
        {
            "AutoCommit" => 0,
            "RaiseError" => 1
        }
    ) or die DBI->errstr;

my $db_invoices = DBI->connect("dbi:Pg:dbname=$dbname_invoices;host=$host;port=$port",
        $username,
        $password,
        {
            "AutoCommit" => 0,
            "RaiseError" => 1
        }
    ) or die DBI->errstr;

$db_organizations->do("truncate kontur.organizations");
$db_invoices->do("truncate kontur.invoices");

sub gen_name {
    my $name; map { $name .= $letters[int(rand($letters_count))] } (1..10);
    $name;
}

sub gen_inn {
    my $inn; map { $inn .= $numbers[int(rand($numbers_count))] } (1..12);
    $inn;
}

sub gen_period {
    my $y = int rand(10) + 10;
    my $m = int rand(11) + 1;
    my $q = int rand(3)  + 1;
    return sprintf("y%d[%02d]q[%d]", $y, $m, $q);
}

sub gen_type {
    int rand(5) + 8;
}

sub gen_date {
    my $y = int rand(10) + 10;
    my $m = int rand(11) + 1;
    my $d = int rand(30) + 1;
    sprintf("20%02d-%02d-%02d", $y, $m, $d);
}

sub gen_number {
    my $number;
    map { $number .= $characters[int(rand($characters_count))] } (1..50);
    $number;
}

my $id_org = 0;
my $id_inv = 0;

for (1..1_000_000) {
    $id_org++;

    my ($inn_org, $name_org) = (gen_inn, gen_name);

    $db_organizations->do("INSERT INTO kontur.organizations (id, inn, name) VALUES (?,?,?)", {}, $id_org, $inn_org, $name_org);

    my $rand_invoices = int rand(10) + 5;

    my $owner_inns = $db_organizations->selectall_arrayref(
            "SELECT inn FROM kontur.organizations WHERE id<>? ORDER BY random() limit ?",
            {}, $id_org, $rand_invoices
        );

    my $contractor_inns = @$owner_inns ? $db_organizations->selectall_arrayref(
            "SELECT inn FROM kontur.organizations WHERE id NOT IN (" . (join ',' => split // => ("?" x @$owner_inns)) . ") ORDER BY random() limit ?",
            {}, (map { $_->[0] } @$owner_inns), $rand_invoices
        ) : [];

    for (my $index = 0; $index < scalar @$owner_inns; $index++) {
        my $owner_inn = $owner_inns->[$index] ? $owner_inns->[$index][0] : 0;
        my $contractor_inn = $contractor_inns->[$index] ? $contractor_inns->[$index][0] : 0;

        last unless $contractor_inn;

        my %data = (
            "owner_inn"      => $owner_inn,
            "contractor_inn" => $contractor_inn,
            "period_inv"     => gen_period,
            "type_inv"       => gen_type,
            "date_inv"       => gen_date,
            "number_inv"     => gen_number,
        );

        my @data_sort = qw/period_inv owner_inn type_inv contractor_inn date_inv number_inv/;

        my $json_inv = "{" . (join ',' => map { qq<"$_":"$data{$_}"> } @data_sort ) . "}";

        push @data_sort, "json_inv";        
        $data{"json_inv"} = $json_inv;

        $db_invoices->do(
            "INSERT INTO kontur.invoices (period, owner_inn, type, contractor_inn, date, number, json) VALUES (?,?,?,?,?,?,?)",
            {}, (map { $data{$_} } @data_sort)
        );
    }
}