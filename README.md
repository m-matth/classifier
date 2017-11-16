```a poc program

poc [COMMAND] ... [OPTIONS]
  manage elastic search poc : api server and utlities

Commands:
  import   import json data to elastic search
  export   export json data to trans/bdd
  server   serve document similarity api
  hotswap  index bsearch data and hotswap alias index

Common flags:
  -? --help     Display help message
  -V --version  Print version information
The poc program




poc import [OPTIONS]
  import json data to elastic search

Flags:
  -i --input=FILE          bsearch json data file
     --es-host=URL         es uri (http://x.x.x.x:9200)
     --es-user=USERNAME    es username (default elastic)
     --es-passwd=PASSWORD  es password (default changeme)
     --es-idx=STRING       es index to work on
     --es-map=STRING       es map to work on
Common flags:
  -? --help                Display help message
  -V --version             Print version information
The poc program




poc server [OPTIONS]
  serve document similarity api

Flags:
  -p --port=PORT           listen port
     --es-host=URL         es uri (http://x.x.x.x:9200)
     --es-user=USERNAME    es username (default elastic)
     --es-passwd=PASSWORD  es password (default changeme)
     --es-idx=STRING       es index to work on
     --es-map=STRING       es map to work on
     --bsearch-host=HOST   bsearch host
     --bsearch-port=PORT   bsearch port
Common flags:
  -? --help                Display help message
  -V --version             Print version information

