# AUTO WSJT-X FT8 & FT4 COMMUNICATION

This is a script to automate ft8 and ft4 using WSJT-X that already modified that can be downloaded [here](https://bit.ly/WSJTX-Fahadh).

### Requirements
* [adif-io](https://pypi.org/project/adif-io/)
* [pyhamtools](https://pypi.org/project/pyhamtools/)
* [pymongo](https://pypi.org/project/pymongo/)
* [python-dotenv](https://pypi.org/project/python-dotenv/)
* [redis](https://pypi.org/project/redis/)
* [tqdm](https://tqdm.github.io/)

### Environment (.env)
* `WSJTX_IP`, IP of UDP connection from WSJT-X
* `WSJTX_PORT`, Port of UDP connection from WSJT-X
* `MULTICAST`, set `TRUE` if multicast enabled from WSJT-X
* `MONGO_HOST`, IP of MongoDB database
* `MONGO_PORT`, Port of MOngoDB database
* `REDIS_HOST`, IP of Redis database
* `REDIS_PORT`, Port of Redis database
* `QRZ_API_KEY`, API key from QRZ account to access QRZ log (optional)
* `QRZ_USERNAME`, callsign from QRZ account to access another callsign data (optional)
* `QRZ_PASSWORD`, password from QRZ account (optional)