import json, pprint, requests, textwrap, time
#host = 'http://stanford1.fyre.ibm.com:8998'
host = 'http://audi1.fyre.ibm.com:8998'
data = {'kind': 'spark'}
headers = {'Content-Type': 'application/json'}

def get_session_state(session_url):
    response = requests.get(session_url, headers=headers)
    response_json = response.json()
    state = None
    if 'state' in response_json:
        state = response_json['state']
    return state, response_json

# 1. creating a session
r = requests.post(host + '/sessions', data=json.dumps(data), headers=headers)
response = r.json()
print(response)

# 2. get the session id
session_url = host + '/sessions/' + str(response['id'])
session_state = None
response_json = None
while session_state != 'idle':
    session_state, response_json = get_session_state(session_url)
    print('session_state:'+str(session_state))
    if session_state != 'idle':
        # not yet gone to idle state
        time.sleep(5)
    else:
        break

pprint.pprint(response_json)

# 3. submit the code for execution
statements_url = session_url + '/statements'
print('\n>>>>>>>>>>>>>>>>>>>>>\n')
print('job url:'+str(statements_url))
print('\n>>>>>>>>>>>>>>>>>>>>>\n')
data = {
  'code': textwrap.dedent("""
    val NUM_SAMPLES = 100000;
    val count = sc.parallelize(1 to NUM_SAMPLES).map { i =>
      val x = Math.random();
      val y = Math.random();
      if (x*x + y*y < 1) 1 else 0
    }.reduce(_ + _);
    println(\"Pi is roughly \" + 4.0 * count / NUM_SAMPLES)
    """)
}
r = requests.post(statements_url, data=json.dumps(data), headers=headers)
print('\n>>>>>>>>>>>>>>>>>>>>>\n')
print('job submission response')
pprint.pprint(r.json())
print('\n>>>>>>>>>>>>>>>>>>>>>\n')

statements_url = session_url + '/statements/' + str(response['id'])
print('statements_url:'+str(statements_url))

# 4. get the statement execution status
session_state = None
response_json = None
while session_state != 'available':
    session_state, response_json = get_session_state(statements_url)
    print('session_state:'+str(session_state))
    if session_state != 'available':
        # not yet gone to idle state
        time.sleep(5)
    else:
        break

print('\n>>>>>>>>>>>>>>>>>>>>>\n')
print('job response')
pprint.pprint(response_json)
print('\n>>>>>>>>>>>>>>>>>>>>>\n')

response = requests.delete(session_url, headers=headers)
print(response)