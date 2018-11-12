# Lebowski

A Couchbase implementation of the Bridges protocols.

![TheDude on Couch](http://i.imgur.com/Gv8XHUe.jpg)

## What does it do?



## Development

You can interact with the library in the REPL by typing in Emacs:

    M-x cider-jack-in
    user> 

Initialize the components by: 

    user> (dev)
    dev> (reset)


## Deploying 

First, setup your GPG credentials and Leiningen environment.

See these for details:

https://github.com/technomancy/leiningen/blob/master/doc/GPG.md
https://github.com/technomancy/leiningen/blob/master/doc/DEPLOY.md#authentication

### TL;DR

To release a snapshot:

    $ lein release :patch

To release a minor version:

    $ lein release :minor
    
To release a major version:

    $ lein release :major
