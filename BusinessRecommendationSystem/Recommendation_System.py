from py2neo import Node, Relationship
from py2neo import authenticate, Graph
import csv
from Yelp_User import Yelp_User
from Yelp_Business import Yelp_Business
import datetime
# this python script researches various types of recommendations and Natural language processing that can be
# performed in Graph Db using cypher query language. context based filtering, collaborative filtering are deeply
# analysed. collaborative filtering is done through calculation of cosine similarity between two users.
# NLP sentiment analysis is performed using the APOC procedures and Graph Aware's NLP procedures plugins which are
# manually inserted into the database.



# Class for all Methods of the Recommendation System ranging from CRUD operations to querying recommendations
# for a particular user
class Recommendation_System:

    username = 'neo4j' # UserID for the local Neo4j database
    password = 'Rika@123'  # Password to the Neo4j database
    url = 'http://localhost:7474'# Url to access the graph database system

    if username and password:
        authenticate(url.strip('http://'), username, password)
    graph = Graph(url + '/db/data/')# Create neo4j Graph object

    # *----------------------------------------------------------------------------------------------------------------- * /
    # *------------------------------------------CREATION--------------------------------------------------------------- * /
    # *----------------------------------------------------------------------------------------------------------------- * /

    # Method to insert multiple users into the graph database:
    # Input: .csv file with all users information in the format attached in Data folder
    # Output: Graph created with all User as Nodes with following properties:
    # 1. User Id: Random 12 character string
    # 2. User Name: Name of the User
    # 3. Review Count: Number of Reviews
    # 4. Yelping_since: Year since the user is yelping

    def create_user_using_csv(self,filename):
        # Open csv file
        with open(filename, mode='r') as csv_file:
            # Reading csv file to csv_reader
            csv_reader = csv.reader(csv_file)
            line_count = 0
            # Looping over csv_reader to read each line into Yelp_User object
            for i, row in enumerate(csv_reader):

                if i == 0:#Ignore Header
                    continue
                user_id = row[0]
                name = row[1]
                review_count = row[2]
                yelping_since = row[3]
                user = Yelp_User(user_id,name,yelping_since,review_count)
                user_Node = Node("Yelp_User1", user_id=user.user_id, user_name=user.user_name, review_count=user.review_count,
                                 yelping_since=user.yelping_since)

                # Creating graph with new Node User
                self.graph.create(user_Node)

    # ----------------------------------------------------------------------------------------------------------------------

    # Method to insert multiple Businesses into the graph database:
    # Input: .csv file with all business information in the format attached in Data folder in project
    # Output: Graph created with all Business as Nodes with following properties:
    # 1. Business Id: Random 12 character string
    # 2. Business Name: Name of the Business
    # 3. Category: Number of Reviews
    # 4. Longitude: Longitude of the business's location
    # 5. Latitude: Latitude of the business's location
    # 6. Address: Address of the business's location
    # 7. City: City of the business's location
    # 8. State: State of the business's location
    # 9. Postal Code: Postal Code of the business's location

    def create_business_using_csv(self,filename):
        graph = Graph(self.url + '/db/data/')
        with open(filename, mode='r') as csv_file:
            csv_reader = csv.reader(csv_file)
            line_count = 0
            for i, row in enumerate(csv_reader):
                if i == 0:
                    continue
                business_id = row[0]
                name = row[1]
                address = row[3]
                city = row[4]
                state = row[5]
                postal_code = row[6]
                latitude = row[7]
                longitude = row[8]
                Category = row[12]
                # Yelp_Business object
                business = Yelp_Business(business_id,name,Category,longitude,latitude,address,city,state,postal_code)
                business_Node = Node("Yelp_Business",
                                     Business_id=business.business_id,
                                     Business_name=business.business_name,
                                     Address=business.address,
                                     City=business.city,
                                     State=business.state,
                                     Postal_code=business.postal_code,
                                     Latitude=business.latitude,
                                     Longitude=business.longitude,
                                     Category=business.category)
                graph.create(business_Node)

    # ----------------------------------------------------------------------------------------------------------------------

    # Method to insert multiple Category Nodes into the graph database:
    # Input: .csv file with all Category information in the format attached in Data folder in project
    # Output: Graph created with all Category as Nodes with following properties:
    # 1. Category Name: Name of the Category, Check data for possible values
    def create_category_using_csv(self, filename):
        graph = Graph(self.url + '/db/data/')
        with open(filename, mode='r') as csv_file:
            csv_reader = csv.reader(csv_file)
            line_count = 0
            for i, row in enumerate(csv_reader):
                if i == 0:
                    continue
                category_name = row[0]
                category = Node("Yelp_Category", Category_name=category_name)
                graph.create(category)

    # ----------------------------------------------------------------------------------------------------------------------

    # Method to insert multiple "Reviewed_Business" Relationships into the graph database:
    # Input: .csv file with all information of who reviewed which business see format attached in Data
    # folder in project
    # Output: Graph created with all "Reviewed_Business" Relationships with following properties:
    # 1. ratingStar: Star rating given by user to Business
    # 2. Comment: Reviews given by User to Business
    def create_reviewed_business_relationship(self,filename):
        with open(filename, mode='r') as csv_file:
            csv_reader = csv.reader(csv_file)
            line_count = 0
            for i, row in enumerate(csv_reader):
                if i == 0:
                    continue
                user = row[0]
                business = row[6]
                star = row[7]
                Comment = row[9]
                query = "MATCH (user:Yelp_User {user_id: '" + user + "'}) MATCH (business:Yelp_Business1 {business_id: '" + str(
                    business) + "'}) MERGE (user)-[rv:Reviewed_Business]->(business) ON CREATE SET rv.ratingstars " \
                                "= toInt('" + star + "') rv.Comment = str('"+Comment+"')"
                self.graph.cyphercypher.execute(query)

    # ----------------------------------------------------------------------------------------------------------------------

    # Method to insert multiple "In_Category" Relationships into the graph database:
    # Input: .csv file with all information of which business belongs to which category see format attached in Data
    # folder in project
    # Output: Graph created with all "In_Category" Relationships
    def create_in_category_relationship(self,filename):

        filename = "/Users/chandrikasharma/Documents/data_yelp/user_reviews_v1.csv"
        with open(filename, mode='r') as csv_file:
            csv_reader = csv.reader(csv_file)
            line_count = 0
            for i, row in enumerate(csv_reader):
                if i == 0:
                    continue
                business_id = row[0]
                Category = row[12]
                query = "MATCH (c:Yelp_Category {Category_name: '" + Category + "'}) " \
                    "MATCH (b:Yelp_Business1 {business_id: '" + str(business_id) + "'}) MERGE (b)-[rv:In_Category]->(c);"
                self.graph.cypher.execute(query)

    # ----------------------------------------------------------------------------------------------------------------------

    # Method to insert one Business into the graph database:
    # Input:
    # 1. Business Id: Random 12 character string
    # 2. Business Name: Name of the Business
    # 3. Category: Number of Reviews
    # 4. Longitude: Longitude of the business's location
    # 5. Latitude: Latitude of the business's location
    # 6. Address: Address of the business's location
    # 7. City: City of the business's location
    # 8. State: State of the business's location
    # 9. Postal Code: Postal Code of the business's location
    # Output: Graph created with one Business Node with the input properties

    def create_one_business(self, business_id,business_name,password,address,city,state,postal_code,latitude,longitude,
                            Category,review_count,stars):
        graph = Graph(self.url + '/db/data/')
        business = Node("Yelp_Business",
                        Business_id=business_id,
                        Business_name=business_name,
                        password = 123,
                        Address=address,
                        City=city,
                        State=state,
                        Postal_code=postal_code,
                        Latitude=latitude,
                        Longitude=longitude,
                        Category=Category,
                        Average_Stars=stars)
        graph.create(business)

    # ----------------------------------------------------------------------------------------------------------------------

    # Method to insert one user into the graph database:
    # Input:
    # 1. User Id: Random 12 character string
    # 2. User Name: Name of the User
    # 3. Review Count: Number of Reviews
    # 4. Yelping_since: Year since the user is yelping
    # Output: Graph created with all User as Node with input properties

    def create_one_user(self, user):
        # Open csv file
        #
        user_Node = Node("Yelp_User", user_id=user.user_id, user_name=user.user_name,# Create a Yelp_User Node here
                         review_count=user.review_count,
                         yelping_since=user.yelping_since)

        # Creating graph with new Node User
        self.graph.create(user_Node)# Insert Yelp_User Node into the Graph
    # ----------------------------------------------------------------------------------------------------------------------

    # Method to insert a new Category Node into the graph database:
    # Input: Parameters needed to create a category Node in graph:
    # 1. Category Name: Name of the Category, Check data for possible values
    # Output: Graph created with one new Category as Node with input properties
    def create_one_category(self, category_name):
        category = Node("Yelp_Category", Category_name=category_name)
        self.graph.create(category)

    # ----------------------------------------------------------------------------------------------------------------------

    # Method to insert Single "In_Category" Relationships into the graph database:
    # Input:
    # 1. BusinessId
    # 2. Category Name
    # Output: New "In_Category" Relationship created between a Category Node and a Business Node
    def create_single_in_category_relationship(self, business_id,Category):
        query = "MATCH (c:Yelp_Category {Category_name: '" + Category + "'}) MATCH (b:Yelp_Business1 {business_id: '" \
                + str(business_id) + "'}) MERGE (b)-[rv:In_Category]->(c);"
        self.graph.cypher.execute(query)

    # ----------------------------------------------------------------------------------------------------------------------

    # Method to insert single "Reviewed_Business" Relationship into the graph database:
    # Input:
    # business_name: name of the Business
    # Ratingstar: Number of Stars assigned by User
    # Comment: Review comments given by a User
    # User_Name: UserName of the reviewer
    # Output: New "Reviewed_Business" relationship
    def create_single_reviewed_business_relationship(self, business_id,star,user_name,Comment):
        query = "MATCH (user:Yelp_User {user_id: '" + user_name + "'}) MATCH (business:Yelp_Business1 {business_id: '" \
                + str(business_id) + "'}) MERGE (user)-[rv:Reviewed_Business]->(business) ON CREATE SET rv.ratingstars " \
                                     "= toInt('" + star + "') rv.Comment = str('" + Comment + "')"
        self.graph.cyphercypher.execute(query)
    #----------------------------------------------------------------------------------------------------------------------

    # Method to insert multiple "Friends_With" Relationships into the graph database this is a bidirectional relationship:
    # Input: .csv file with all information of who is friends with whome. See format attached in Data
    # folder in project
    # Output: Graph created with all "Friends_With" Relationships
    def create_friends_relationships_using_csv(self,filename):
        graph = Graph(self.url + '/db/data/')
        with open(filename, mode='r') as csv_file:
            csv_reader = csv.reader(csv_file)
            line_count = 0
            for i, row in enumerate(csv_reader):
                if i == 0:
                    continue
                cypher = graph.cypher
                user_id = row[2]
                friend_user_id = row[3]
                query = "MATCH(u1:Yelp_User{user_id:'"+user_id+"'}) MATCH(u2:Yelp_User{user_id:'"+friend_user_id+"'}) " \
                                                               "MERGE(u1) - [rv:Friends_With]-(u2);"
                cypher.execute(query)

    # ----------------------------------------------------------------------------------------------------------------------

    # Method to insert single "Friends_With" Relationships into the graph database:
    # Input:
    # UserId of first user
    # UserId of second user
    # Output: Graph created with all "Friends_With" Relationship between User1 and User2
    def create_single_friends_relationship(self, user_id,friend_user_id):
        query = "MATCH(u1:Yelp_User{user_id:'" + user_id + "'}) MATCH(u2:Yelp_User{user_id:'" + friend_user_id + "'}) " \
                                                                               "MERGE(u1) - [rv:Friends_With]-(u2);"
        self.graph.cypher.execute(query)

    # *------------------------------------------------------------------------------------------------------------------ * /
    # *------------------------------------------READ OPERATIONS--------------------------------------------------------- * /
    # *------------------------------------------------------------------------------------------------------------------ * /

    # Method to read all Yelp_Users in graph
    # Input: Current instance
    # Output: List of all Yelp_Users with set properties

    def get_all_users(self):
        query = '''
                MATCH(a:Yelp_User)
                return a;
                '''
        return self.graph.cypher.execute(query)

    # *----------------------------------------------------------------------------------------------------------------- * /

    # Method to read all Yelp_Users in graph
    # Input: Current instance
    # Output: List of all Yelp_Users with set properties

    def get_one_users(self,user_id):
        query = " MATCH(a:Yelp_User{user_id:'"+user_id+"'}) return a.user_name; "
        return self.graph.cypher.execute(query)

    # *----------------------------------------------------------------------------------------------------------------- * /

    # Method to read all business nodes in graph
    # Input: Current instance
    # Output: List Of All Yelp_Businesses with set properties
    def get_all_business(self):
        query = '''
                MATCH(a:Yelp_Business)
                return a;
                '''
        return self.graph.cypher.execute(query)

    # *----------------------------------------------------------------------------------------------------------------- * /

    # Method to get details of one business using business_id in graph
    # Input: business_id
    # Output: List Of All properties for Yelp_Businesses

    def get_one_business(self,business_id):

        query = "MATCH(a:Yelp_Business{business_id:'"+business_id+"'}) return a;"

        return self.graph.cypher.execute(query)

    # *----------------------------------------------------------------------------------------------------------------- * /
    # Method to get details of one business using business_id in graph
    # Input: business_id
    # Output: Business name of the business id
    def get_business_name(self,business_id):

        query = "MATCH(a:Yelp_Business{business_id:'"+business_id+"'}) return a.business_name;"

        return self.graph.cypher.execute(query)
    # *------------------------------------------------------------------------------------------------------------------* /

    # Method to read all "Reviewed_Business" relationships in graph for a particular user
    # Input: User name of the Yelp_User
    # Output: List Of All Yelp_Businesses that the Yelp_User has reviewed

    def get_all_reviewed_business_of_user(self, user_name):
        query = "MATCH (u:Yelp_User{user_name:'"+user_name+"'})-[r:Reviewed_Business]->(b) RETURN b;"
        return self.graph.cypher.execute(query)

    # -------------------------------------------------------------------------------------------------------------------* /

    # Method to read and output all category nodes in graph
    # Input: Current instance
    # Output: List of All Yelp_Categories
    def get_all_category(self):
        query = '''
                MATCH(a:Yelp_Category)
                return a;
                '''
        return self.graph.cypher.execute(query)

    # *------------------------------------------------------------------------------------------------------------------* /

    # Method to read and output a business's category in graph
    # Input: Current instance
    # Output: Returns the category a business belongs to

    def get_business_category(self,business_id):
        query = "MATCH(b:Yelp_Business)-[r:In_Category]->(c) return c;"
        return self.graph.cypher.execute(query)

    # *------------------------------------------------------------------------------------------------------------------* /

    # Method to read all "Reviewed_Business" relationships in graph for a particular user
    # Input: User name of the Yelp_User
    # Output: List Of All Yelp_Businesses that the Yelp_User has reviewed

    def get_all_friends_of_user(self, user_name):
        query = "MATCH (u:Yelp_User{user_name:'" + user_name + "'})-[r:Friends_with]-(b) RETURN b;"
        return self.graph.cypher.execute(query)

    # *----------------------------------------------------------------------------------------------------------------- * /
    # *------------------------------------------UPDATE OPERATIONS------------------------------------------------------ * /
    # *----------------------------------------------------------------------------------------------------------------- * /
    # Method is used to update the properties of a Yelp_User
    # Input: Property to be updated
    # Value: New Value of the property

    def update_user(self,user_id,param,value):
        query = ""
        if param == 'user_name':
            query = "MATCH(n:Yelp_User{user_id:'" + user_id + "'})SET n.user_name = '"+value+"' RETURN n;"
        elif param =='yelping_since':
            query = "MATCH(n:Yelp_User{user_id:'"+user_id+"'})SET n.yelping_since = '"+value+"' RETURN n;"
        elif param == 'review_count':
            query = "MATCH(n:Yelp_User{user_id:'" + user_id + "'})SET n.review_count = '" + value + "' RETURN n;"
        return self.graph.cypher.execute(query)
    # ------------------------------------------------------------------------------------------------------------------

    # Method is used to update the properties of a Yelp_Business
    # Input: Property inside a business node to be updated
    # Value: New Value of the property
    def update_business(self, user_id, property, value):
        query =""
        if property == 'business_name':
            query = "MATCH(n:Yelp_Business{business_id:'" + user_id + "'})SET n.business_name = '" + value + "' RETURN n;"
        elif property == 'address':
            query = "MATCH(n:Yelp_Business{business_id:'" + user_id + "'})SET n.address = '" + value + "' RETURN n;"
        elif property == 'state':
            query = "MATCH(n:Yelp_Business{business_id:'" + user_id + "'})SET n.state = '" + value + "' RETURN n;"
        elif property == 'postal_code':
            query = "MATCH(n:Yelp_Business{business_id:'" + user_id + "'})SET n.postal_code = '" + value + "' RETURN n;"
        elif property == 'city':
            query = "MATCH(n:Yelp_Business{business_id:'" + user_id + "'})SET n.city = '" + value + "' RETURN n;"
        elif property == 'Category':
            query = "MATCH(n:Yelp_Business{business_id:'" + user_id + "'})SET n.Category = '" + value + "' RETURN n;"
        elif property == 'Average_stars':
            query = "MATCH(n:Yelp_Business{business_id:'" + user_id + "'})SET n.Average_stars = '" + value + "' RETURN n;"

        return self.graph.cypher.execute(query)

    # ------------------------------------------------------------------------------------------------------------------
    # Method to update the relationship properties, the relationship "Reviewed_Business" has property:
    # stars reviewed and Comments
    def update_reviewed_business_properties(self,user_id, property, value):
        query = ""
        if property == "ratingstars":
            query = "MATCH(n:Yelp_User{user_id:'" + user_id + "'})-[r:Reviewed_Business]->(b:Yelp_Business)" \
                                                              "SET r.ratingstars = '" + value + "' RETURN b.business_name;"

    # ------------------------------------------------------------------------------------------------------------------

    # ----------------------------------------------------------------------------------------------------------------------
    # This Method enables a user to review a business with comments and stars, When a user reviews a business,
    # a relationship gets created between the user node and business node with "Reviewed Business" relationship name
    # This also leads to creating a ratings star property on the relationship so we know that the user rated with how many stars.
    # Input: Yelp_User ID
    # Function: Relationship gets created between the user and business andstar gets assigned to that relationship
    # Also the business re-calculates its average rating since a new rating has been added to it.

    def create_user_reviewed_business(self,user_id,business_id,star):
        query2=" "
        # graph = Graph(self.url + '/db/data/')
        query="MATCH (a:Yelp_User{user_id:'"+user_id+"'}) MATCH(b:Yelp_Business{business_id:'"+business_id+"'}) " \
         "MERGE (a)-[r:Reviewed_Business]->(b) ON CREATE SET r.ratingstars= '"+str(star)+ "' RETURN r;"


        business_reviewed = self.graph.cypher.execute(query)
        # business re - calculates its average rating since a new rating has been added to it
        if business_reviewed:
            print("-[:reviewed_business]-> Successfully created")
            query2 = "MATCH(b:Yelp_Business {business_id: '"+business_id+"'}) < -[r:Reviewed_Business]-() " \
            "WITH b, avg(toInt(r.ratingstars)) AS avg_rating SET b.avg_rating = avg_rating return b;"
        return self.graph.cypher.execute(query2)

    # *----------------------------------------------------------------------------------------------------------------- * /
    # *------------------------------------------DELETE OPERATIONS------------------------------------------------------ * /
    # *----------------------------------------------------------------------------------------------------------------- * /

    # method to delete a user when given a user id
    def delete_user(self,user_id):
        query = ("MATCH (u;Yelp_user {user_id:'" + user_id + "'}) DELETE u")
        return self.graph.cypher.execute(query)

    # method to delete a business when given a business id
    def delete_business(self,business_id):
        query = ("MATCH (b:Yelp_Business {business_id:'" + business_id+"'}) DELETE b")
        return self.graph.cypher.execute(query)

    # method to delete a relationship between a user and a business based on user id and business id
    def delete_user_business_relation(self,user_id,business_id):
        query= ("MATCH (u:Yelp_user {user_id:'" + user_id + "'})-[r:Reviewed_Business]->(b:Yelp_Business {business_id:'"
                                                            +business_id+"'}) DELETE r")
        return self.graph.cypher.execute(query)

    # method to delete a category
    def delete_category(self,category_id):
        query = ("MATCH (c:Category {Category_id:'" + category_id + "'})")
        return self.graph.cypher.execute(query)



    # *------------------------------------------------------------------------------------------------------------- * /
    # *------------------------------------------RECOMMENDATION SYSTEM OPERATIONS----------------------------------- * /
    # *------------------------------------------------------------------------------------------------------------- * /
    # This function takes in the user id of the user and retrieves all the similar businesses that the user is
    # currently viewing based on the matches in a particular category
    # Input: user id of the user
    # Output: recommended business names
    # Function: this function takes in the user id of the user and retrieves all the similar businesses that the user is
    #           currently viewing based on the matches in a particular category
    def get_recommendation_context_based(self, user_id):
        graph = Graph(self.url + '/db/data/')
        query = "MATCH (u:Yelp_User{user_id:'" + user_id + "'})-[rv:Reviewed_Business]->(b:Yelp_Business)-[:In_Category]->" \
                                                           "(c:Yelp_Category)<-[:In_Category]-(rec:Yelp_Business) " \
                                                           "WHERE NOT EXISTS ((u)-[:Reviewed_Business]->(rec)) " \
                                                           "WITH rec, [b.business_name,COUNT(*)] AS scores " \
                                                           "RETURN rec.business_name," \
                                                           "COLLECT(scores) AS scoreComponents," \
                                                           "REDUCE (s=0,x in COLLECT(scores) | s+x[1]) AS score " \
                                                           "ORDER BY score DESC LIMIT 10;"

        return graph.cypher.execute(query)

    # Input: user_id of the user
    # Output: recommended business
    # Function:
    # this function takes in the user id of the user and simulates the similarity score based on the businesses
    # visited by the other user and scores are calculated accordingly. The outputs are sorted based on the score calculated
    # The graph structure allows to retrieve the set of users similar to u1, without browsing the whole set of users.Likewise, the
    # set of movies seen by the similar users but not by u1 can be retrieved in a very simple and direct way, which makes graph
    # databases well suited for that kind of approaches.

    def collaborative_filtering_step_1(self, user_id):
        graph = Graph(self.url + '/db/data')
        query = ("MATCH (u1:Yelp_User {user_id:'" + user_id + "'})-[:Reviewed_Business]->(b1:Yelp_Business) with count(b1) as countb "
                "MATCH (u1:Yelp_User {user_id:'" + user_id + "'})-[:Reviewed_Business]->(b1:Yelp_Business) "
                "MATCH (b1)<-[r:Reviewed_Business]-(u2:Yelp_User) "
                "WHERE NOT u2=u1 "  
                " WITH u2, countb, toFloat(count(r))/countb as sim "  
                "WITH count(u2) as countu, countb "             
                "MATCH (u1:Yelp_User {user_id:'" + user_id + "'})-[:Reviewed_Business]->(b1:Yelp_Business) "
                "MATCH (b1)<-[r:Reviewed_Business]-(u2:Yelp_User) "
                "WHERE NOT u2=u1 "
                " WITH u1, u2,countu, toFloat(count(r))/countb as sim "
                "MATCH (b:Yelp_Business)<-[r:Reviewed_Business]-(u2) "
                "WHERE NOT (b)<-[:Reviewed_Business]-(u1) "
                 "RETURN DISTINCT b.business_name, toFloat(count(r))/countu as score,sim ORDER BY score DESC")

        return (graph.cypher.execute(query))

    # *------------------------------------------------------------------------------------------------------------- * /
    # input: user_id of the user
    # output: recommended businesses for the user
    # function: this function takes the user id of the user and calculate the score based on the cosine similarity and
    #             recommends business based on businesses that were being rated by users

    def collaborative_filtering_step_2(self, user_id):
        graph = Graph(self.url + '/db/data')
        # In Step 2, the similarity between two users u1 and u2 is the proportion of businesses they have in common
        # The score of one business m is the sum of ratings given by users similar to u1
        query = (
            ### Similarity normalization : count number of businesses seen by u1 ###
            # Count businesses rated by u1 as countm
                "MATCH (b1:Yelp_Business)<-[:Reviewed_Business]-(u1:Yelp_User {user_id:'" + user_id + "'}) "
                "with count(b1) as countb "
                ### Recommendation ###
                # Retrieve all users u2 who share at least one businesses with u1
                "match (u2:Yelp_User)-[r2:Reviewed_Business]->(b1:Yelp_Business)<-[r1:Reviewed_Business]-(u1:Yelp_User {user_id:'" + user_id + "'}) " \
                "WHERE (NOT u2=u1) AND (abs(r2.ratingstars - r1.ratingstars) <= 1) "
                # Compute similarity
                "WITH u1, u2, tofloat(count(DISTINCT b1))/countb as sim "
                # Retrieve businesses b that were rated by at least one similar user, but not by u1
                "MATCH (b:Yelp_Business)<-[r:Reviewed_Business]-(u2:Yelp_User) "
                "WHERE (NOT (b)<-[:Reviewed_Business]-(u1)) "
                # Compute score and return the list of suggestions ordered by score
                "RETURN DISTINCT b.business_name,tofloat(sum(r.ratingstars)) as score,sim ORDER BY score DESC")
        return (graph.cypher.execute(query))

        # Strategies 1 and 2 are very rough as regards the evaluation of similarity: two users are considered similar provided that they have
        # a sufficient amount of businesses in common, but their respective tastes are actually not compared

        #
        # input: user id of the user
        # output: recommend business for the user
        # function: users are considered similar if they have a sufficient amount of business in common AND if they have often given approximatively
        #          the same ratings to these movies.

        # In Strategy 3, the similarity between two users is the proportion of business for which they gave almost the same rating
        # The score of one business b is the mean rating given by users similar to u1

    def collaborative_filtering_step_3(self, user_id):
        graph = Graph(self.url + '/db/data')
        query = (  ### Similarity normalization : count number of businesses rated by u1 ###
            # Count movies rated by u1 as countm
                "match (b1:Yelp_Business)<-[:Reviewed_Business]-(u1:Yelp_User {user_id:'" + user_id + "'}) "
                "with count(b1) as countb "
                ### Recommendation ###
                # Retrieve all businesses u2 who share at least one business with u1
                "match (u2:Yelp_User)-[r2:Reviewed_Business]->(b1:Yelp_Business)<-[r1:Reviewed_Business]-(u1:Yelp_User {user_id:'" + user_id + "'}) "
                # Check if the ratings given by u1 and u2 differ by less than 1
                "where (not u2=u1) and (abs(r2.ratingstars - r1.ratingstars) <= 1) "
                # Compute similarity
                "with u1, u2, tofloat(count(distinct b1))/countb as sim "
                # Retrieve movies m that were rated by at least one similar user, but not by u1
                "match (b:Yelp_Business)<-[r:Reviewed_Business]-(u2:Yelp_User) "
                "where (not (b)<-[:Reviewed_Business]-(u1)) "
                # Compute score and return the list of suggestions ordered by score
                "WITH DISTINCT b, count(r) as n_u, tofloat(sum(r.ratingstars)) as sum_r,sim "
                "RETURN b.business_name, sum_r/n_u as score,sim ORDER BY score DESC")
        return graph.cypher.execute(query)

    # def business_timeline(self, business_id):
    #     graph = Graph(self.url + '/db/data/')
    #     query = "MATCH (b:Yelp_Business{business_id:'" + business_id + "'})-[r1:In_Category]->(c:Yelp_Category) MATCH (c)<-[r2:In_Category]-(b1:Yelp_Business) where b1.avg_rating>=4 return b1;"
    #     return graph.cypher.execute(query)

    # ----------------------------------------------------------------------------------------------------------------------

    # input:takes the businessid of the business_time
    # output: shows all the positive or negative sentences
    # function: this function annotates the review with POS tags and tags them with positive or negative sentences

    def get_sentiment_analysis_from_reviews(self, business_id):
        graph = Graph(self.url + '/db/data/')
        query1 = (
            # Detect language and update each review text with that information
            "MATCH (b:Yelp_Business {business_id: {'" + business_id + "' }})<-[r:Reviewed_Business]-(u:Yelp_user)"
            "CALL ga.nlp.detectLanguage(r.text)"
            "YIELD result"
            "SET r.language = result"
            "with r"
        # Annotate all text that's detected as English, as the underlying library may not support things it detects as non-English
            "MATCH ()<-[r { language: "'en'" }]-()"
            "CALL ga.nlp.annotate({text: r.text, id: id(r)})"
            "YIELD result"
        # add the annotated text relationship to the user 
            "MERGE (u:Yelp_User)-[:HAS_ANNOTATED_TEXT]->(result)"
            "with r"
        # perform sentiment analysis on the annotated text
            "MATCH (u:Yelp_user)-[]-(a:AnnotatedText)"
            "CALL ga.nlp.sentiment(a) YIELD result "
            "RETURN result")
        return graph.cypher.execute(query1)
    # ------------------------------------------------------------------------------------------------------------------
    # This method takes advantage of Friend network of the system:
    # It recommends to a user based on the businesses reviewed by their friends:
    # Input: User Id
    # Output: List of businesses based on friends network
    # function: Friend's based recommendations

    def collaborative_filtering_friends_based(self,user_id):
        graph = Graph(self.url + '/db/data/')
        query="MATCH(a:Yelp_User{user_id:'"+user_id+"'})-[:Friends_With]->(b:Yelp_User) " \
                "MATCH(b)-[r:Reviewed_Business]->(business:Yelp_Business) where business.avg_rating>4 return business.business_name;"
        # print query

        return graph.cypher.execute(query)

    # ----------------------------------------------------------------------------------------------------------------------
    # Method takes in business name and password and spits out the top competetions of that business that
    # out should watch out for.
    # input: Business_name
    # output: List of businesses that are competetitions of that business, i.e all top businesses in the same category
    def business_timeline(self,business_id):
        graph = Graph(self.url + '/db/data/')
        query = "MATCH (b:Yelp_Business{business_id:'"+business_id+"'})-[r1:In_Category]->(c:Yelp_Category) MATCH (c)" \
                "<-[r2:In_Category]-(b1:Yelp_Business) where b1.avg_rating>=4 return b1.business_name;"
        return graph.cypher.execute(query)

    # ----------------------------------------------------------------------------------------------------------------------
    # Method takes input user id and outputs the recommended businesses for the Yelp user based on:
    # 1. Collaborative Filtering Recommendations
    # 2. Collaborative Filtering Friendship Based Recommendations
    # 3. Collaborative Filtering Step 1 Based Recommendations
    # 4. Collaborative Filtering Step 2 Based Recommendations
    # 5. Collaborative Filtering Step 3 Based Recommendations

    def user_timelime(self,user_id):

        recommendation_CF = self.get_basic_recommendation_user_collaborative(user_id)
        print "Collaborative Filtering Recommendations:"
        for i in recommendation_CF:
            print str(i).split("\n")[2]
        recommendation_BCBR = self.collaborative_filtering_friends_based(user_id)
        print "Collaborative Filtering Friendship Based Recommendations:"
        for i in recommendation_BCBR:
            print str(i).split("\n")[2]
        recommendation_CF_Step1 = self.collaborative_filtering_step_1(user_id)
        print "Collaborative Filtering Step 1 Based Recommendations :"
        for i in recommendation_CF_Step1:
            print str(i).split("\n")[2]
        try:
            recommendation_CF_Step2 = self.collaborative_filtering_step_2(user_id)
            print "Collaborative Filtering Step 2 Based Recommendations :"
            for i in recommendation_CF_Step2:
                print str(i).split("\n")[2].split('|')
        except:
            print" No Collaborative Filtering Step 2 Based Recommendations"

        try:
            recommendation_CF_Step3 = self.collaborative_filtering_step_3(user_id)
            print "Collaborative Filtering Step 3 Based Recommendations :"
            for i in recommendation_CF_Step3:
                print str(i).split("\n")[2]
        except:
            print 'No Collaborative Filtering Step 3 Based Recommendations'
        context_based = self.get_recommendation_context_based(user_id)
        print"Context Based Recommendations:"
        for i in context_based:
            print str(i).split("\n")[2].split('|')[0]


# ----------------------------------------------------------------------------------------------------------------------

if __name__ == "__main__":
    recommend = Recommendation_System()
    print ("Welcome to the Yelp Business Recommendation System:")
    entity = raw_input("Are you a Yelp User Or a Yelp Business? Enter U for User and B for Business: ")
    print entity
    if entity == "U":
        user = raw_input("Do you have an already existing account? y/n ")
        if user=='y':
            user_id = raw_input("Please enter your user_id here: ")
            print ("Welcome Back: ")+ str(recommend.get_one_users(user_id)).split("\n")[2].split('|')[1]
            print(" Here Is Your Timeline: ")
            # TsgBsn19Wjwpyo81gF9_8Q
            recommend.user_timelime(user_id)
        if user=='n':
            # Asking User to create a new account:

            user_name = raw_input("Please enter your first name to create new account:")
            user_password = raw_input("Please enter your password to create new account:")
            user_id=user_name+'123'
            yelping_since = now = datetime.datetime.now()

            # Creating new User object to insert into the graph as new Yelp_User Node
            user_object = Yelp_User(user_id, user_name, yelping_since, 0)  # Create Yelp_User Object
            recommend.create_one_user(user_object)

            recommend.user_timelime(user_id)

            print("Making the user visit China-Restaurant Wok-House: n0061DPXoCFMKZPgPZDgVw")
            recommend.create_user_reviewed_business(user_id,'n0061DPXoCFMKZPgPZDgVw',5)
            print "Recommendations again:"
            recommend.user_timelime(user_id)


