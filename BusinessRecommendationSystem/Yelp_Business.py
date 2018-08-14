class Yelp_Business:
    def __init__(self,business_id,business_name,category,longitude,latitude,address,city,state,postal_code,Average_stars):
        self.business_id = business_id
        self.business_name = business_name
        self.category=category
        self.longitude = longitude
        self.latitude = latitude
        self.address = address
        self.city = city
        self.state = state
        self.postal_code = postal_code
        self.Average_stars = Average_stars
    def get_business_id(self):
        return self.business_id

    def get_business_name(self):
        return self.business_name

    def get_business_category(self):
        return self.category

    def get_Average_stars(self):
        return self.Average_stars

    def get_business_address(self):
        add = self.address + ", " + self.city + ", " + self.state + ", " + self.postal_code
        return add

    def get_business_ll(self):
        return self.latitude + " ,"+self.longitude

    def set_business_id(self,business_id):
        self.business_id = business_id

    def set_business_name(self,name):
        self.business_name=name

    def set_business_category(self,category):
        self.category=category

    def set_business_address(self,address, city,state,postal_code):
        self.address = address
        self.city =city
        self.state = state
        self.postal_code = postal_code

    def set_business_ll(self,lat,long):
        self.latitude=lat
        self.longitude=long