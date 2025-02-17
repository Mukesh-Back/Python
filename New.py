from neo4j import GraphDatabase

URI = "bolt://localhost:7687/neo4j"
AUTH = ("neo4j", "12345678")

def neo4j_query(driver):
    with driver.session() as session:
        # How do you create nodes representing employees with properties such as name, position, and department?
        session.run("""
            Create (raji:employee {name: "Raji", age: 19, salary: 6000, department: "sales"})
            Create (kathiravan:employee {name: "Kathiravan", age: 21, salary: 10000, department: "sales"})
            Create (sri:employee {name: "Sri", age: 24, salary: 20000, department: "development"})
            Create (rajan:employee {name: "Rajan", age: 23, salary: 20000, department: "development"})
            Create (nehru:employee{name:"Nehru",age:23,salary:28000,department :"development"})
            Create (gandhi:employee{name:"Gandhi",age:23,salary:28000,department :"sales"})
            Create (dharun:team_lead {name: "Dharun", age: 28, salary: 40000, department: "team_lead"})
            Create (selvi:team_lead {name: "Selvi", age: 30, salary: 45000, department: "team_lead"})
            Create (pandi:manager {name: "Pandi", age: 40, salary: 100000, department: "manager"})
        """)
        #How do you retrieve all employees who work in the "Sales" department?
        session.run("Match (n) Where n.department='sales' return n")

        # How do you create relationships where employees report to a manager?
        #How do you find all employees who report to a specific manager, including indirect reports?
        session.run("""
                    match (raji:employee {name: "Raji"}),(kathiravan:employee {name: "Kathiravan"}),(sri:employee {name: "Sri"}),(nehru:employee{name:"Nehru"}),
                    (gandhi:employee{name:"Gandhi"}),(rajan:employee {name: "Rajan"}),(dharun:team_lead{name:"Dharun"}),(selvi:team_lead {name: "Selvi"}),(pandi:manager{name:"Pandi"})
                    
                    Create (raji)-[:REPORT_TO_TEAM_LEAD]->(dharun),
                    (kathiravan)-[:REPORT_TO_TEAM_LEAD]->(dharun),
                    (sri)-[:REPORT_TO_TEAM_LEAD]->(dharun),
                    (nehru)-[:REPORT_TO_TEAM_LEAD]->(selvi),
                    (selvi)-[:REPORT_TO_MANAGER]->(pandi),
                    (dharun)-[:REPORT_TO_MANAGER]->(pandi),
                    (gandhi)-[:REPORT_TO_MANAGER]->(pandi),
                    (rajan)-[:REPORT_TO_MANAGER]->(pandi)

        """)        
        #In a social network, how do you find the person with the most connections?
        session.run(""" Create (maha:person{name:"Maha"}),(ambi:person{name:"Ambi"}),(mathi:person{name:"Mathi"}),(arjun:person{name:"Arjun"}) ,(nanthan:person{name:"Nanthan"})  with nanthan
                    
                    match (maha:person{name:"Maha"}),(ambi:person{name:"Ambi"}),(mathi:person{name:"Mathi"}),(arjun:person{name:"Arjun"}) ,(nanthan:person{name:"Nanthan"})

                    create (arjun)-[:CONNECTED_ON]->(mathi),
                    (nanthan)-[:CONNECTED_ON]->(arjun),
                    (mathi)-[:CONNECTED_ON]->(maha),
                    (ambi)-[:CONNECTED_ON]->(maha)                    
                    """)
        #In a social network, how do you find the person with the most connections?
        session.run("""MATCH (p:person)-[r:CONNECTED_ON]->() RETURN p, COUNT(r) AS connections ORDER BY connections DESC LIMIT 1 """)

        #How do you retrieve the top 3 most connected people?
        session.run("MATCH (p:person)-[r:CONNECTED_ON]->() RETURN p, COUNT(r) AS connections ORDER BY connections DESC LIMIT 3")

        # Given a "Person" node with FRIEND relationships, how do you find people who are friends of friends but are not directly connected?
        session.run("Match (s:person)-[:CONNECTED_ON]->(sf:person)-[:CONNECTED_ON]->(t:person) where not (s)-[:CONNECTED_ON]->(t) and s<>t return s,t")

        #How do you find an actor who has collaborated with the most other actors?
        session.run(""" CREATE (a1:actor {name: "Praveen"}),
                            (a2:actor {name: "Sri"}),
                        (a3:actor {name: "Rajan"}),
                        (m1:movie {name: "ABC"}),
                        (m2:movie {name: "Kanguva"}),
                        (m3:movie {name: "Kan"})""")

        session.run(""" MATCH (b1:actor {name: "Praveen"}), 
                        (b2:actor {name: "Sri"}), 
                        (b3:actor {name: "Rajan"}), 
                        (n1:movie {name: "ABC"}), 
                        (n2:movie {name: "Kanguva"}), 
                        (n3:movie {name: "Kan"})
                    CREATE (b1)-[:ACTED]->(n1)<-[:ACTED]-(b3),
                         (b2)-[:ACTED]->(n2)<-[:ACTED]-(b1),
                         (b1)-[:ACTED]->(n3) """)

        
        session.run("MATCH (p:actor)-[:ACTED]->(m:movie)<-[:ACTED]-(p1:actor)  WHERE p <> p1  RETURN p, m, p1 ")

        #Given a graph of cities and direct flights, how do you find the shortest path between two cities?
        session.run("""CREATE (c:city {name:"Chennai"}),
                        (c1:city {name:"Covai"}),
                        (c2:city {name:"Madurai"}) WITH c, c1, c2
                    MATCH (c:city {name:"Chennai"}), 
                        (c1:city {name:"Covai"}), 
                        (c2:city {name:"Madurai"})
                    CREATE (c)-[:Flight {distance: 500}]->(c1),
                            (c1)-[:Flight {distance: 200}]->(c2),
                            (c2)-[:Flight {distance: 800}]->(c) WITH c
                    MATCH (s:city {name: "Chennai"}), 
                        (e:city {name: "Madurai"}), 
                        path = shortestPath((s)-[:Flight*]->(e))  RETURN path

        """)
        #How do you calculate the shortest connection between two employees in a corporate hierarchy?
        session.run("""
            CREATE (e1:employee {name: "Ariya", title: "CEO"}),
            (e2:employee {name: "Suriya", title: "Manager"}),
            (e3:employee {name: "Variya", title: "Developer"}),
            (e4:employee {name: "Kiya", title: "Developer"}),
            (e5:employee {name: "Chiya", title: "Manager"}) WITH e1, e2, e3, e4, e5
            CREATE (e1)-[:MANAGES]->(e2),
            (e1)-[:MANAGES]->(e5),
            (e2)-[:MANAGES]->(e3),
            (e2)-[:MANAGES]->(e4) WITH e1, e2, e3, e4, e5
            MATCH (start:employee {name: "Ariya"}), (end:employee {name: "Kiya"}), path = shortestPath((start)-[:MANAGES*]->(end)) 
            RETURN path
""")
        # How do you find the top 5 most Purchased products?
        session.run("""Create (c1:customer{name:"Sam"}),(c2:customer{name:"Vam"}),(c3:customer{name:"Dam"}),
                    (p1:Product {name: "Laptop"}),(p2:Product {name: "Smartphone"}),(p3:Product {name: "Tablet"}),
                    (p4:Product {name: "Headphones"}),(p5:Product {name: "Camera"}),(p6:Product {name: "Monitor"}) with p6


                    match (c1:customer{name:"Sam"}),(c2:customer{name:"Vam"}),(c3:customer{name:"Dam"}),
                    (p1:Product {name: "Laptop"}),(p2:Product {name: "Smartphone"}),(p3:Product {name: "Tablet"}),
                    (p4:Product {name: "Headphones"}),(p5:Product {name: "Camera"}),(p6:Product {name: "Monitor"})


            CREATE (c1)-[:Purchased]->(p1),(c1)-[:Purchased]->(p2),
                (c2)-[:Purchased]->(p2),(c2)-[:Purchased]->(p3),
                (c3)-[:Purchased]->(p1),
                (c3)-[:Purchased]->(p4),
                (c1)-[:Purchased]->(p5),
                (c2)-[:Purchased]->(p5),
                (c3)-[:Purchased]->(p5),
                (c1)-[:Purchased]->(p6),
                (c2)-[:Purchased]->(p6) with p6


                MATCH (c:customer)-[r:Purchased]->(p:Product) RETURN p.name AS product, COUNT(r) AS purchase_count
                    ORDER BY purchase_count DESC
                    LIMIT 5
                    """)
        
        #How do you determine which products are commonly bought together by the same customers?
        session.run("""        
                    MATCH (c:customer)-[:Purchased]->(p1:Product), (c)-[:Purchased]->(p2:Product) WHERE id(p1) < id(p2) RETURN p1.name AS product1, p2.name AS product2, COUNT(*) AS together_count ORDER BY together_count DESC LIMIT 5
""")
        #How do you suggest new friends to a user based on their existing friends-of-friends network?
        session.run("""        
                    Match (p:person {name: 'Ambi'})-[:CONNECTED_ON]->(friend)-[:CONNECTED_ON]->(fof)
                    Where NOT (p)-[:CONNECTED_ON]->(fof) AND p <> fof
                    return fof.name AS RecommendedFriend
        """)

        # How do you avoid suggesting people they are already connected with?

    #   session.run("""Match (p1:person), (p2:person) WHERE id(p1) <> id(p2) AND NOT EXISTS((p1)-[:CONNECTED_TO]-(p2)) RETURN p2""")
        #Given a financial transaction graph, how do you find users who have transferred money through multiple intermediate accounts in a short period?
        session.run("""Create (u1:user{name:"Prasanth"}),(u2:user{name:"Kumar"}),
                    (b1:bank{name:"BOB",account:123}),(b2:bank{name:"SBI",account:879}),
                    (b3:bank{name:"UBI",account:321}),(b4:bank{name:"IOB",account:345}) with b4
                    
                    Match (u1:user{name:"Prasanth"}),(u2:user{name:"Kumar"}),
                    (b1:bank{name:"BOB",account:123}),(b2:bank{name:"SBI",account:879}),
                    (b3:bank{name:"UBI",account:321}),(b4:bank{name:"IOB",account:345})
                    
                    Create (u1)-[:Transfered{amount:20000,Transfered_date:date("2024-06-01")}]->(b1)-[:Transfered{amount:3999,Transfered_date:date("2024-06-03")}]->(b2),
                    (u2)-[:Transfered{amount:10000,Transfered_date:date("2024-05-03")}]->(b3)-[:Transfered{amount:10000,Transfered_date:date("2024-05-08")}]->(b4),
                    (u1)-[:Transfered{amount:10000,Transfered_date:date("2024-06-08")}]->(b2),
                    (u2)-[:Transfered{amount:10000,Transfered_date:date("2024-05-10")}]->(b4) with b4

                    match (us:user)-[r:Transfered]-(bk:bank) where r.Transfered_date >date("2024-04-30") and r.Transfered_date < date("2024-07-01") 
                    return us,bk

                    """)
        #How do you identify users who have an unusually high number of transactions compared to the average?
        session.run("""Match (u1:user{name:"Prasanth"}),(u2:user{name:"Kumar"}),
                    (b1:bank{name:"BOB",account:123}),(b2:bank{name:"SBI",account:879}),
                    (b3:bank{name:"UBI",account:321}),(b4:bank{name:"IOB",account:345})
                    
                    match (us:user)-[r:Transfered]-(bk:bank)  WITH us, COUNT(r) AS Transfered
        WITH avg(Transfered) AS avgTransfered
                     MATCH (us:user)-[r:Transfered]-(bk:bank)
        WITH us, COUNT(r) AS Transfered, avgTransfered
        WHERE Transfered > avgTransfered
        RETURN us, Transfered
                    """)
        
        #In a graph where nodes represent people and edges represent influence (e.g., follows, mentorship), how do you find the most influential person?
        session.run("""Create (i1:influencer{name:"Ram"}),(i2:influencer{name:"Seetha"}),
                    (i3:influencer{name:"Sarav"}),(i4:influencer{name:"Mukil"}),(i5:influencer{name:"Guru"}) with i5

                    match (i1:influencer{name:"Ram"}),(i2:influencer{name:"Seetha"}),
                    (i3:influencer{name:"Sarav"}),(i4:influencer{name:"Mukil"}),(i5:influencer{name:"Guru"}) with i5

                    Create (i1)-[:Follows]->(i2)<-[:Follows]-(i3),
                    (i1)-[:Metorship]->(i2)<-[:Metorship]-(i3),
                    (i2)-[:Follows]->(i4)<-[:Follows]-(i5),
                    (i2)-[:Metorship]->(i4)<-[:Metorship]-(i5)


                    MATCH (p:influencer)-[r:Follows|Metorship]->() RETURN p, COUNT(r) AS Most_influential_person ORDER BY Most_influential_person DESC LIMIT 1

""")
        
        # How do you rank people based on their influence using PageRank or another centrality measure?
        session.run("""""")


        #How do you identify people in a social network who have significantly more connections than the average user?
        session.run("""MATCH (p:person)-[r:CONNECTED_ON]-(b:person)
        WITH p, COUNT(r) AS connections
        WITH avg(connections) AS avgConnections
        
        MATCH (p:person)-[r:CONNECTED_ON]-(b:person)
        WITH p, COUNT(r) AS connections, avgConnections
        WHERE connections > avgConnections
        RETURN p, connections
""") 
            

with GraphDatabase.driver(URI, auth=AUTH) as driver:
    neo4j_query(driver)
