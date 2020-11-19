#!/usr/bin/perl
use strict;
use warnings;
use LWP::UserAgent ();
use Test::More tests => 3;
use JSON::XS;
use FindBin qw<$Bin>;
use lib "$Bin/../lib";
use Utils qw<csv_to_arrayref>;
use DDP;

my ($json, $response, $content, $result);
my $url = "http://46.36.223.44:9090/";
my $params = {
    "name"   => "emb",
    "period" => "y15[03]q[3]",
    "type"   => 11,
};

my $ua = LWP::UserAgent->new("timeout" => 10);

# Require Kontur
#
require_ok("Kontur");

# way = in
#
$params->{"way"} = "in"; $json = encode_json $params;
$response = $ua->get($url, "Content" => $json);
ok($response->is_success, "Connect (way = in)");
$content = $response->decoded_content;
$result  = Utils::csv_to_arrayref($content);
ok($result->[0]{"owner_inn"} == 250842755889, "Data [owner_inn]");
ok($result->[0]{"number"} eq "e1xe9s3e75lrl33k0vztczjcsf1nyo139yxsl1ei26j8ms88fp", "Data [number]");
ok($result->[0]{"type"} == 11, "Data [type]");

# way = copy
#
$params->{"way"} = "copy"; $json = encode_json $params;
$response = $ua->get($url, "Content" => $json);
ok($response->is_success, "Connect (way = copy)");
$content = $response->decoded_content;
$result  = Utils::csv_to_arrayref($content);
ok($result->[0]{"owner_inn"} == 250842755889, "Data [owner_inn]");
ok($result->[0]{"number"} eq "e1xe9s3e75lrl33k0vztczjcsf1nyo139yxsl1ei26j8ms88fp", "Data [number]");
ok($result->[0]{"type"} == 11, "Data [type]");