The poc program

poc [COMMAND] ... [OPTIONS]

Common flags:
     --es-host=URL         es uri (http://x.x.x.x:9200)
     --es-user=USERNAME    es username (default elastic)
     --es-passwd=PASSWORD  es password (default changeme)
  -? --help                Display help message
  -V --version             Print version information

poc import [OPTIONS]
  import json data to elastic search

  -i --input=FILE          bsearch json data file

poc server [OPTIONS]
  serve document similarity api

  -p --port=PORT           listen port
     --bsearch-host=HOST   bsearch host
     --bsearch-port=PORT   bsearch port
