#!/usr/bin/perl
use strict;
use warnings;
use 5.010;
use Benchmark qw(:all);
use AnyEvent::HTTP;
use JSON::XS;
use DDP;

my @letters = ('a'..'z');
my $letters_count = scalar @letters;
my $url = "http://46.36.223.44:9090/";

sub gen_name {
    my $name; map { $name .= $letters[int(rand($letters_count))] } (1..int rand (5) + 1);
    $name;
}

sub gen_type {
    int rand(5) + 8;
}

my $res_200 = {};

sub get {
    my $cv = AnyEvent->condvar;

    my $way = shift;

    my $params = {
        "way"  => $way,
        "name" => gen_name,
        "type" => gen_type,
    };

    my $json = encode_json $params;

    foreach (1..100) {
        $cv->begin;

        http_request(
            "GET"  => $url,
            "body" => $json,
            sub {
                my ($body, $hdr) = @_;

                $res_200->{$way}++ if $hdr->{"Status"} == 200;

                $cv->end;
            }
        );
    }

    $cv->recv;
}

timethese(2, {
    'Way [in]' => sub {get("in")},
    'Way [copy]' => sub {get("copy")},
});

p $res_200;