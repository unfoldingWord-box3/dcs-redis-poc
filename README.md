# tX Redis POC

This is a tX pipeline redis simulation

To run:

* Download Docker if you don't have it

* Clone this repo: `git clone https://github.com/unfoldingWord-box3/tx-redis-poc``

* Run `docker compose up --build`

* Go to [http://localhost:4000](http://localhost:4000) in your browser

* Enter how any seconds a started job should take (will sleep this long)

* Select if you want to schedule the job for a later time (simulates a user [non-master] branch) and enter how many second later.

* Click "Queue Job"

* Reload the page often to see your job move through the registries

* Queue the job again with the same payload before your old job finishes to see it cancel that old job

* Queue a job with a different "ref" or different "repo.full_name" to see your old job still remain and this new one be added

CREDIT: App from https://blog.abbasmj.com/implementing-redis-task-queues-and-deploying-on-docker-compose
