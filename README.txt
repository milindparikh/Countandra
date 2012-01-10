Since Aryabhatta invented zero, Mathematicians such as John von Neuman have been in pursuit of efficient counting and architects have constantly built systems that computes counts quicker. In this age of social media, where 100s of 1000s events take place every second, we were inspired by twitter's Rainbird project to develop distributed counting engine that can scale linearly.

Countandra is a hierarchical distributed counting engine on top of Cassandra (to increment/decrement hierarchical data) and Netty (HTTP Based Interface). It provides a complete http based interface to both posting events and getting queries. The syntax of a event posting is done in a FORMS compatible way. The result of the query is emitted in JSON to make it maniputable by browsers directly.
Features

    Geographically distributed counting.
    Easy Http Based interface to insert counts.
    Hierarchical counting such as com.mywebsite.music.
    Retrieves counts, sums and square in near real time.
    Simple Http queries provides desired output in JSON format
    Queries can be sliced by period such as LASTHOUR,LASTYEAR and so on for MINUTELY,HOURLY,DAILY,MONTHLY values
    Queries can be classified for anything in hierarchy such as com, com.mywebsite or com.mywebsite.music
    Open Source and Ready to Use!

Countandra 0.5 uses Cassandra 1.0.1 as the distributed database to insert the events and retrieve the data. The http based interface is provided through Netty 3.2.5. Internally Countandra uses Hector 1.0.1 to interface with Cassandra and makes extensive use of Joda time utilities. Countandra is open source and ready to use!

There is only one way of posting events.
HTTP POST {data}
{data} = c=pc&s=com.mywebsite.music&t=1320555720000&v=200

There is only one way of getting results of queries.
HTTP GET {query}
{query} = /pc/com.mywebsite/TODAY/HOURLY



================Installation Steps============================
Requirements:

    Java >= 1.6
    Apache Ant

Installation steps for multi node countandra:
Steps to set up first node

    Download countandra.
    Set up countandra properties at config/countandra.properties
        cassandraHostIp: Specify localhost ip
        cassandraPort: Port on which cassandra should run. Default port is 9160.
        cassandraDirectory:Path to directory where all data should be stored
        cassandraSeeds: Specify nodes which you want to act as seeds for our mutli node countandra.
    Run setupCountandra.sh
    Run ant runcountandrainit

Steps to set up other nodes of cluster

    Download countandra.
    Set up countandra properties at config/countandra.properties
        cassandraHostIp: Specify localhost ip
        cassandraPort: Port on which cassandra should run. Default port is 9160.
        cassandraDirectory:Path to directory where all data should be stored
        cassandraSeeds: Specify nodes which you want to act as seeds for our mutli node countandra.
    Run setupCountandra.sh
    Run ant runcountandra


