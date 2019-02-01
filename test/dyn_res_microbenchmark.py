import ray
import time
ray.init()

N=1000

start = time.time()
for i in range(0,N):
    ray.experimental.create_resource(str(i), 1)
end = time.time()

print("Time taken to submission = " + str(end-start))
print(len(ray.global_state.available_resources()))  # This might not show all resources - wait for sometime and try again. todo: put this in a check loop
