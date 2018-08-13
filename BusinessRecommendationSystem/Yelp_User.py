class Yelp_User:
    def __init__(self,user_id,user_name,yelping_since,review_count):
        self.user_id = user_id
        self.user_name =user_name
        self.yelping_since = yelping_since
        self.review_count = review_count
    def get_user_id(self):
        return self.user_id
    def get_user_name(self):
        return self.user_name
    def get_yelping_since(self):
        return self.yelping_since
    def get_review_count(self):
        return self.review_count
    def set_username(self,user_name):
        self.user_name = user_name
    def set_user_id(self,id):
        self.user_id=id
    def set_review_count(self,review_count):
        self.review_count=review_count
    def set_yelping_since(self,since):
        self.yelping_since = since



