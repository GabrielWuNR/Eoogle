class cache_all_search():
    def __init__(self):
        self.previous = ""
        self.cache_list = []

    def get_cache(self, start_page):
        start_list=start_page * 10
        return self.cache_list[start_list:start_list+10]

    def set_cache(self, new_cache_list):
        self.cache_list = new_cache_list