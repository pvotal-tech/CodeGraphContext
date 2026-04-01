from google.cloud import spanner
from google.cloud.spanner_v1.transaction import Transaction

client = spanner.Client(project='test')
instance = client.instance('test')
db = instance.database('test')
t = Transaction(db.session())
try:
    print(t.batch_update([("UPDATE tbl SET val = 1", {}, {})]))
except Exception as e:
    print("FIRST", e)
try:
    print(t.batch_update([("UPDATE tbl SET val = @v", {"v": 1}, {"v": spanner.param_types.INT64})]))
except Exception as e:
    print("SECOND", type(e), e)
