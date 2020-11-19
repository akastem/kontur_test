package Utils;
use strict;
use warnings;
use 5.010;
use JSON::XS;
use Exporter 'import';

our @EXPORT_OK = qw<csv_to_json csv_to_arrayref>;

sub csv_to_json { encode_json csv_to_arrayref($_[0]) }

sub csv_to_arrayref {
    my $csv = shift;

    my $array = [];

    my $keys_line = $1 if $csv =~ s/(.*?)\n//;
    my @keys = split ";" => $keys_line;

    foreach my $line (split "\n" => $csv) {
        my %hash;     
        my @values = split ";" => $line;

        @hash{@keys} = @values;

        push(@$array, \%hash);
    }

    return $array;
}

1;