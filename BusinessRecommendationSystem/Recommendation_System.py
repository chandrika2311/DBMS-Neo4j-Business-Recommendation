from py2neo import Node, Relationship
from py2neo import authenticate, Graph
import csv

class Recommendation_System:
    username = 'neo4j'
    password = 'Rika@123'
    url = 'http://localhost:7474'

    if username and password:
        authenticate(url.strip('http://'), username, password)
    graph = Graph(url + '/db/data/')

# ----------------------------------------------------------------------------------------------------------------------
    def create_user_using_csv(self,filename):
        graph = Graph(self.url + '/db/data/')
        with open(filename, mode='r') as csv_file:
            csv_reader = csv.reader(csv_file)
            line_count = 0
            for i, row in enumerate(csv_reader):
                if i == 0:
                    continue
                user_id = row[0]
                name = row[1]
                review_count = row[2]
                yelping_since = row[3]
                user = Node("Yelp_User1", user_id=user_id, user_name=name, review_count=review_count,
                            yelping_since=yelping_since)
                graph.create(user)

# ----------------------------------------------------------------------------------------------------------------------
    def create_uniqueness_constraints(self):

        graph = Graph(self.url + '/db/data/')
        query = "CREATE CONSTRAINT ON (u:Yelp_User1) ASSERT u.user_id IS UNIQUE;"
        graph.cypher.execute(query)

# ----------------------------------------------------------------------------------------------------------------------
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
                stars = row[9]
                Category = row[12]
                business = Node("Yelp_Business1",
                                Business_id=business_id,
                                Business_name=name,
                                Address=address,
                                City=city,
                                State=state,
                                Postal_code=postal_code,
                                Latitude=latitude,
                                Longitude=longitude,
                                Category=Category,
                                Stars = stars)
                graph.create(business)

# ----------------------------------------------------------------------------------------------------------------------
    def create_category_using_csv(self, filename):
        graph = Graph(self.url + '/db/data/')
        with open(filename, mode='r') as csv_file:
            csv_reader = csv.reader(csv_file)
            line_count = 0
            for i, row in enumerate(csv_reader):
                if i == 0:
                    continue
                category_name = row[0]
                category = Node("Yelp_Category1", Category_name=category_name)
                graph.create(category)

# ----------------------------------------------------------------------------------------------------------------------
    def create_reviewed_business_relationship(self,filename):
        graph = Graph(self.url + '/db/data/')
        with open(filename, mode='r') as csv_file:
            csv_reader = csv.reader(csv_file)
            line_count = 0
            for i, row in enumerate(csv_reader):
                if i == 0:
                    continue
                cypher = graph.cypher
                user = row[0]
                business = row[6]
                star = row[7]
                date = row[8]
                Comment = row[9]
                query = "MATCH (user:Yelp_User1 {user_id: '" + user + "'}) MATCH (business:Yelp_Business1 {business_id: '" + str(
                    business) + "'}) MERGE (user)-[rv:Reviewed_Business]->(business) ON CREATE SET rv.ratingstars = toInt('" + star + "')"
                cypher.execute(query)

# ----------------------------------------------------------------------------------------------------------------------
    def create_incategory_relationship(self,filename):

        graph = Graph(self.url + '/db/data/')
        filename = "/Users/chandrikasharma/Documents/data_yelp/user_reviews_v1.csv"
        with open(filename, mode='r') as csv_file:
            csv_reader = csv.reader(csv_file)
            line_count = 0
            for i, row in enumerate(csv_reader):
                if i == 0:
                    continue
                cypher = graph.cypher
                business_id = row[0]
                Category = row[12]
                query = "MATCH (c:Yelp_Category1 {Category_name: '" + Category + "'}) MATCH (b:Yelp_Business1 {business_id: '" + str(
                    business_id) + "'}) MERGE (b)-[rv:In_Category]->(c);"
                cypher.execute(query)

# ----------------------------------------------------------------------------------------------------------------------
    def create_one_business(self, business_id,business_name,password,address,city,state,postal_code,latitude,longitude,Category,review_count,stars):
        graph = Graph(self.url + '/db/data/')
        business = Node("Yelp_Business1",
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
                query = "MATCH(u1:Yelp_User{user_id:'"+user_id+"'}) MATCH(u2:Yelp_User{user_id:'"+friend_user_id+"'}) MERGE(u1) - [rv:Friends_With]->(u2);"
                cypher.execute(query)

# ----------------------------------------------------------------------------------------------------------------------
    def create_in_category_relationship_using_csv(self,filename):
        graph = Graph(self.url + '/db/data/')
        with open(filename, mode='r') as csv_file:
            csv_reader = csv.reader(csv_file)
            line_count = 0
            for i, row in enumerate(csv_reader):
                if i == 0:
                    continue
                business_id = row[0]
                Category = row[12]
                query = "MATCH(b:Yelp_Business {business_id: '"+business_id+"}')MATCH(c:Yelp_Category{Category_name:'"+Category+"'})"
                "MERGE(b) - [rv:In_Category]->(c);"
                graph.cypher.execute(query)

# ----------------------------------------------------------------------------------------------------------------------
    def create_one_user(self, user_id,user_name,password=123,review_count=0,yelping_since = 2018):
        graph = Graph(self.url + '/db/data/')
        user = Node("Yelp_User1", User_Id=user_id, User_Name=user_name, Password= password,review_count=review_count,
                    Yelping_Since=yelping_since)
        graph.create(user)
# ----------------------------------------------------------------------------------------------------------------------
    def get_recommendation_user_collaborative(self):
        graph = Graph(self.url + '/db/data/')
        query = '''
        MATCH(a:Yelp_User{user_name:"Cathy"})-[:Reviewed_Business]->(b:Yelp_Business)<-[:Reviewed_Business]-(u:Yelp_User)
        MATCH(u)-[:Reviewed_Business]->(rec:Yelp_Business) where not exists((a)-[:Reviewed_Business]->(rec))
        return rec;
        '''
        return graph.cypher.execute(query)

# ----------------------------------------------------------------------------------------------------------------------

    def get_recommendation_context_based(self):
        graph = Graph(self.url + '/db/data/')
        query = '''
        MATCH (u:Yelp_User{user_name:"Jacob"})-[rv:Reviewed_Business]->(b:Yelp_Business)-[:IN_CATEGORY]->(c:Yelp_Category)<-[:IN_CATEGORY]-(rec:Yelp_Business)
        WHERE NOT EXISTS ((u)-[:Reviewed_Business]->(rec))
        WITH rec, [b.business_name,COUNT(*)] AS scores
        RETURN rec.business_name,
        COLLECT(scores) AS scoreComponents,
        REDUCE (s=0,x in COLLECT(scores) | s+x[1]) AS score
        ORDER BY score DESC LIMIT 10;
        '''

        query1='''MATCH (u:Yelp_User{user_name:"Mark"})-[rv:Reviewed_Business]->(b:Yelp_Business)-[:IN_CATEGORY]->(c:Yelp_Category)<-[:IN_CATEGORY]-(rec:Yelp_Business)
                RETURN rec;'''
        return graph.cypher.execute(query1)

# ----------------------------------------------------------------------------------------------------------------------
    def create_user_reviewed_business(self,user_name,business_id,star):
        # business_reviewed=None
        query2=" "
        graph = Graph(self.url + '/db/data/')
        query="MATCH (a:Yelp_User{user_name:'"+user_name+"'}) MATCH(b:Yelp_Business{business_id:'"+business_id+"'}) MERGE (a)-[r:Reviewed_Business]->(b) ON CREATE SET r.ratingstars = toInt('" + star + "')RETURN b;"

        business_reviewed = graph.cypher.execute(query)
        if business_reviewed:
            query2 = "MATCH(b:Yelp_Business {business_id: '"+business_id+"'}) < -[r:Reviewed_Business]-() WITH b, avg(r.ratingstars) AS avg_rating SET b.avg_rating = avg_rating return b;"
        return graph.cypher.execute(query2)
# ----------------------------------------------------------------------------------------------------------------------
    def collaborative_filtering_friends_based(self):
        graph = Graph(self.url + '/db/data/')
        query='''
        MATCH(a:Yelp_User{user_name:"Cathy"})-[:Friends_With]->(b:Yelp_User)
        MATCH (b)-[r:Reviewed_Business]->(business:Yelp_Business)
        where r.ratingstars=5
        return business;
        '''
        return graph.cypher.execute(query)

    def business_timeline(self,business_id):
        graph = Graph(self.url + '/db/data/')
        query = "MATCH (b:Yelp_Business{business_id:'"+business_id+"'})-[r1:In_Category]->(c:Yelp_Category) MATCH (c)<-[r2:In_Category]-(b1:Yelp_Business) where b1.avg_rating>=4 return b1;"
        return graph.cypher.execute(query)
# ----------------------------------------------------------------------------------------------------------------------

if __name__ == "__main__":
    recommend = Recommendation_System()
    # recommend.create_uniqueness_constraints()
    # recommend.create_user_using_csv("/Users/chandrikasharma/Documents/data_yelp/yelp_users_v1.csv")
    # recommend.create_business_using_csv("/Users/chandrikasharma/Documents/data_yelp/yelp_business.csv")
    # recommend.create_category_using_csv("/Users/chandrikasharma/Documents/data_yelp/category.csv")
    # recommend.create_friends_relationships_using_csv("/Users/chandrikasharma/Documents/data_yelp/friends.csv")
    # print "collaborating filtering"
    # a = recommend.get_recommendation_user_collaborative()
    # print a
    # print "context based"
    # b=recommend.get_resommendation_context_based()

    # recommend.get_user_reviewed_business("Cathy", "UXPgUZ-3ywG_tNKG41kaag")
    # print b