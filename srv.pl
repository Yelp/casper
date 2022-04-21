#!/usr/bin/env perl

use Mojolicious::Lite;

any '/*p' => {p => ''} => sub {
    my $c = shift;
    say $c->app->dumper($c->req->headers->to_hash);
    $c->res->headers->header('x-routing-service' => '10-69-97-238-uswest2aprod; site=admin');
    $c->render(json => {
        method => $c->req->method,
        path => $c->req->url->path,
        headers_in => $c->req->headers->to_hash,
    },
        status => $c->param('status') || 200,
    );
};

app->start;
