import rexerclient
from rexerclient import rql
from rexerclient.rql import *
from rexerclient import client
from rexerclient.models import action
import time
from rexerclient import models
from rexerclient import value

url = 'http://ad3946bfc322c4a23ac0f2546f1f6ea9-559200516.ap-south-1.elb.amazonaws.com/data'
c = client.Client(url)

uid = 12312
video_id = 456
city = value.String('delhi')
gender = value.Int(1)
age_group = value.Int(3)

# for entity which is of type "user" and user_id 12312, set "age" to be 31

c.set_profile("user", uid, "city", city)
c.set_profile("user", uid, "gender", gender)
c.set_profile("user", uid, "age_group", age_group)

print(c.get_profile("user", uid, "city"))
print(c.get_profile("user", uid, "gender"))
print(c.get_profile("user", uid, "age_group"))

# Total views gained by a Trail on last 2 days for given city+gender+age_group
q = Var('args').actions.apply(
  Ops.std.filter(where=it.action_type == String('view'))).apply(
  Ops.std.addProfileColumn(name=String('city'), otype=String('user'), oid=it.actor_id, key=String('city'), default=String(''))).apply(
  Ops.std.addProfileColumn(name='gender', otype=String('user'), oid=it.actor_id, key=String('gender'), default=Int(3))).apply(  
  Ops.std.addProfileColumn(name='age_group', otype=String('user'), oid=it.actor_id, key=String('age_group'), default=Int(7))).apply(
  Ops.std.addColumn(name='key', value=List(it.target_id, it.city, it.gender, it.age_group)))

options = models.aggregate.AggOptions()
duration = 3600*24*2 # 2 days
c.store_aggregate('rolling_counter', 'trail_view_by_city_gender_agegroup_2days_v1', q, duration=duration)


c.log(actor_type='user', actor_id=uid, target_type='video', target_id=video_id, action_type='view', request_id=1, 
      timestamp=int(time.time()), metadata=value.Dict(device_type=value.String('android')),
     )


c.aggregate_value('rolling_counter', 'trail_view_by_city_gender_agegroup_2days_v1', 
                  value.List(value.Int(4124), value.String('delhi'), value.Int(1), value.Int(3)),
                 )
