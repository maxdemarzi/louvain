# Louvain Modularity 

Louvain Modularity Clustering Algorithm for Neo4j

This is an unmanaged extension with a Graph Clustering Algorithm on top of Neo4j.

# Quick Start

1. Build it:

        mvn clean package

2. Copy target/fraud-1.0-SNAPSHOT.jar to the plugins/ directory of your Neo4j server.

        mv target/fraud-1.0-SNAPSHOT.jar neo4j/plugins/.


3. Configure Neo4j by adding a line to conf/neo4j-server.properties:

        org.neo4j.server.thirdparty_jaxrs_classes=com.maxdemarzi=/v1

4. Start Neo4j server.

On the Neo4j Browser:


5. Warm up the database (optional):

        :GET /v1/service/warmup    
            
6. Run the algorithm

        :GET /v1/service/louvain_method
        
7. See all nodes
        
        :GET /v1/service/dump

Loading Data


http://www.docgraph.com/docgraph-team-v2/edughdrqxo42nd/

Download CMS teaming data from https://downloads.cms.gov/foia/physician-shared-patient-patterns-2015-days30.zip
Unzip the file.

Get Unique NPIs for Providers:

        awk -F ',' '{print $1}' physician-shared-patient-patterns-2015-days30.txt > nodes.txt
        awk -F ',' '{print $2}' physician-shared-patient-patterns-2015-days30.txt >> nodes.txt
        perl -ne 'print unless $seen{$_}++' nodes.txt > providers.csv

Prepare provider file by adding npi:ID(Provider) to the top of providers.csv.  Using sed:

        sed -i -e '1i\
                npi:ID(Provider)
                ' providers.csv

Prepare relationship file by adding headers. Using sed:

        sed -i -e '1i\
        :START_ID(Provider),:END_ID(Provider),transactions:int,patients:int,max_day:int
        ' physician-shared-patient-patterns-2015-days30.txt

Remove unwanted blanks from file:

        sed -e "s/ //g" physician-shared-patient-patterns-2015-days30.txt > shared_members.csv

Run Import:

        neo4j-enterprise-2.3.2/bin/neo4j-import --into neo4j-enterprise-2.3.2/data/graph.db --nodes:Provider providers.csv --relationships:SHARE_MEMBER shared_members.csv


TO-DO:
        Improve performance, maybe use a cache ( see https://github.com/ben-manes/caffeine )