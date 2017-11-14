```
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
The poc program

poc [COMMAND] ... [OPTIONS]

Common flags:
  -? --help                   Display help message
  -V --version                Print version information

poc import [OPTIONS]
  import json data to elastic search

  -i --input=FILE             bsearch json data file
     --es-host=URL            es uri (http://x.x.x.x:9200)
     --es-user=USERNAME       es username (default elastic)
     --es-passwd=PASSWORD     es password (default changeme)

poc export [OPTIONS]
  export json data to trans/bdd

  -i --input=FILE             bsearch json data file
     --trans-host=HOST        trans host
     --trans-port=PORT        trans port
     --admin=USERNAME         admin username
     --admin-passwd=PASSWORD  admin passwd

poc server [OPTIONS]
  serve document similarity api

  -p --port=PORT              listen port
     --es-host=URL            es uri (http://x.x.x.x:9200)
     --es-user=USERNAME       es username (default elastic)
     --es-passwd=PASSWORD     es password (default changeme)
     --bsearch-host=HOST      bsearch host
     --bsearch-port=PORT      bsearch port
```