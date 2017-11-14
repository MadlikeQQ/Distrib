# Distributed Systems
This project is made entirely on Java 7.
This project is a part of the course [Distributed Systems] at [NTUA] and is non-commercial.

# What is the project about?
A simple Distributed Hash Table (DHT), based on Chord routing protocol, is designed in which the following functions are implemented:
   - Division of the space of Ids (nodes and objects)
  - Ring Routing 
  - Insert of new nodes
  - Departure of nodes
  - Replication of data

The application is similar to an emulator that initiates multiple nodes of DHT. Every node implements all the functions of DHT, like creating server and client processes, opening sockets and respond to queries. Finally, a simple variation of the Chord routing protocol is used where the insertion and departure of nodes is handled appropriately.

[//]: # (These are reference links used in the body of this note and get stripped out when the markdown processor does its job. There is no need to format nicely because it shouldn't be seen. Thanks SO - http://stackoverflow.com/questions/4823468/store-comments-in-markdown-syntax)
[NTUA]: <https://www.ntua.gr/el/> 
[Distributed Systems]:<http://www.cslab.ntua.gr/courses/distrib/> 
