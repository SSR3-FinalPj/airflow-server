

import base64
with open('/home/user1/Rookies/AirflowService/dag_factory.py', 'r') as f:
    content = f.read()
encoded_content = base64.b64encode(content.encode('utf-8')).decode('utf-8')
with open('/home/user1/Rookies/AirflowService/encoded_dag_factory.txt', 'w') as f:
    f.write(encoded_content)