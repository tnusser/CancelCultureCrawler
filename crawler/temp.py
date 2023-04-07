from api_endpoints import ApiEndpoints
api = ApiEndpoints()

ids = []
with open("temp_data.txt") as file:
    for line in file.readlines():
        splitted = line.split(",")
        ids.append(splitted[1].rstrip("\n"))

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

for el in chunks(ids, 100):
    api.get_users_by_id()