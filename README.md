# tools
Various Single-System tools.

# device-emulator
Groovy script for devices activity emulation. Requires:
 - java-8 or later
 - groovy-3.0.8 or later

Script is located at `src/main/groovy/device-emulator.groovy`. Implemented as a CLI application with POSIX-like classic
interface. For usage help run:
```shell
$ ./device-emulator.groovy -h
```
## usage examples
 - Indefinitely send random temperature read events from single device each 5 seconds to default 
url (`http://localhost:8100`), temperature values are random:
```shell
$ ./device-emulator.groovy
```
 - Indefinitely send random temperature read events from single device each 5 seconds to specific URL, temperature 
values are random:
```shell
$ ./device-emulator.groovy --url='https://webhook.site/8e42e574-04c6-43fe-a35c-4b41905e8e83'
```
 - Indefinitely send temperature read (random temperature) events from 100 devices each 10 seconds, devices are 
not sending all at the same time, but send time is distributed evenly instead:
```shell
$ ./device-emulator.groovy --num-devices=100 --interval=10s \
--url='https://webhook.site/8e42e574-04c6-43fe-a35c-4b41905e8e83'
```
 - Emulate 100 devices which send only 4 events each with 3s interval:
```shell
$ ./device-emulator.groovy -n 100 -i 3s --num-events=4 --url='https://webhook.site/8e42e574-04c6-43fe-a35c-4b41905e8e83'
```
 - Emulate 25 devices with 30s interval which send no more than 1000 events in total:
```shell
$ ./device-emulator.groovy -n 25 -i 30s --max-total-events=1000 --url='https://webhook.site/8e42e574-04c6-43fe-a35c-4b41905e8e83'
```
 - Emulate 10 devices sending temperature read (random temperature) events for 45 seconds:
```shell
$ ./device-emulator.groovy -n 10 --duration=45s --url='https://webhook.site/8e42e574-04c6-43fe-a35c-4b41905e8e83'
```
 - Emulate device sending temperature read events with random temperature between 34 and 42 degrees:
```shell
$ ./device-emulator.groovy --from=34 --to=42
```
- Emulate 2 devices with specific device IDs. Note that `device-ids` option overrides `--num-devices` option, number of
devices will be same as number of device IDs specified in the list:
```shell
device-emulator -i 1s -n 10 --device-ids=device-1,device-2 -m 10 --url 'https://webhook.site/3214d789-3ede-4c03-9ff0-a34d6f27bdbf'
```
 - Emulate 10 devices sending temperature read events with random temperature for max 100 events in total, 
max 30 events per device, and maximum for 80 seconds - whichever condition is met first, the script will stop 
generating events:
```shell
$ ./device-emulator.groovy --num-devices=10 --type=temperature_read --data-pattern=random --from=34 --to=42 \
--interval=10s --max-total-events=100 --num-events=30 --duration=80s \
--url='https://webhook.site/8e42e574-04c6-43fe-a35c-4b41905e8e83'
```
