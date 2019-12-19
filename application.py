from flask import Flask
# from urllib.request import Request, urlopen
# from ssl import _create_unverified_context
import requests
app = Flask(__name__)

@app.route("/")
def hello():
    return "Hello World999!zzzzz!!"

# @app.route("/register")
# def register():
#     para_data = {
#         "profile": {
#             "firstName": "application201111",
#             "lastName": "okta111",
#             "email": "atul.gautam111@cardinalhealth.com",
#             "login": "application201.okta111@cah.com"
#         },
#         "groupIds": ["o-cordis-customers"]
#     }
#     headers = {'Authorization': 'SSWS 00Vyd70nQIOJlxKD2ToO3lVUbYRgvuXMxGuDfkVRwO'}
#     try:
#         #context = _create_unverified_context()
#         with urlopen(
#             Request("https://cardinalb2b.oktapreview.com/api/v1/users?activate=false", headers=headers, method="POST"),
#             data=para_data
#         ) as response:
#             return response.read()
#     except Exception as e:
#         return "error: %s" % e