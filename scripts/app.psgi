#!/usr/bin/perl
use strict;
use warnings;
use FindBin qw<$Bin>;
use Plack::Builder;
use lib "$Bin/../lib";
use Kontur;

my $k = Kontur->new(
    "connect_organizations" => {
        "dbname" => "kontur_organizations",
        "schema" => "kontur",
        "host" => "localhost",
        "port" => "5432",
        "user" => "postgres",
        "password" => "*****",
        "debug" => 1
    },
    "connect_invoices" => {
        "dbname" => "kontur_invoices",
        "schema" => "kontur",
        "host" => "localhost",
        "port" => "5432",
        "user" => "postgres",
        "password" => "*****",
        "debug" => 1
    },
    "connect_redis" => {
        "host" => "127.0.0.1",
        "port" => "6379",
        "EX"   => "10",
    },
    "way" => "copy",
    "debug" => 1,
);

$SIG{"INT"}  = sub { undef $k; die "[DESTROY] CTRL^C"; };
$SIG{"TERM"} = sub { undef $k; die "[DESTROY] PROCESS KILLED"; };

my $app = sub { $k->startup($_[0]) };

builder {
    enable "Plack::Middleware::Static",
       path => qr{^/(favicon.ico)},
       root => "$Bin/static";
    $app;
};