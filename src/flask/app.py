from flask import Flask, render_template
from flask import request, Response
app = Flask(__name__,template_folder='templates')

## Index for html app route
@app.route('/')
def index():
    return render_template("index.html")

@app.route('/search', methods=["GET","POST"])
def search():
    term=request.args.get('search_query')
    datalist=[{
  "brand": "Ford",
  "model": "Focus",
  "year": 2012
},{
  "brand": "Ford",
  "model": "Mustang",
  "year": 1964
},{
  "brand": "BMW",
  "model": "S3",
  "year": 1968
}]
    return render_template("search.html",data=datalist,term=term)
## Running app in debug mode
if __name__ == '__main__':
    app.run(host='0.0.0.0',port=9000,debug=True)
