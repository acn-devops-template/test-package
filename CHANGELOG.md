# Current

<!--next-version-placeholder-->

## v0.6.0 (2023-01-29)
### Feature
* :sparkles: multiple config ([`ff24c84`](https://github.com/datax-tmp/datax-utils-deployment-helper/commit/ff24c84c8a15a158c72f507654e0ef3f586489dd))
* :boom: auto_parameter to use multiple config approach ([`a7af73b`](https://github.com/datax-tmp/datax-utils-deployment-helper/commit/a7af73b7ebb82fbf5f0ab50acbde1e5b3ca732c8))
* :boom: Multiple configuration types ([`47c4eac`](https://github.com/datax-tmp/datax-utils-deployment-helper/commit/47c4eac40036c169fde50f3a3e36f9bdb9f7ac63))
* :boom: change conf structure ([`015f3a7`](https://github.com/datax-tmp/datax-utils-deployment-helper/commit/015f3a7a0fcfd44ca28b2134c581348c3bb165cd))

### Documentation
* :memo: Check auto_parameter example notebook correctness ([`cc0d5ec`](https://github.com/datax-tmp/datax-utils-deployment-helper/commit/cc0d5ecaaace4d20d7cb1bef453c2e455323764a))
* :memo: update docstrings ([`f5f89a3`](https://github.com/datax-tmp/datax-utils-deployment-helper/commit/f5f89a3b2e9d46bf51bf68b8834381014dd510b5))
* :memo: update example notebook ([`50dbb84`](https://github.com/datax-tmp/datax-utils-deployment-helper/commit/50dbb8439537291b14c5bb900f6983ac5ffa4a29))

## v0.5.0 (2022-12-16)
### Feature
* :sparkles: add converter/path_adjuster ([`0fb8c71`](https://github.com/datax-tmp/datax-utils-deployment-helper/commit/0fb8c71f6fc5b3262884b60de8a85a812ed1ec93))
* :sparkles: common (Task) and utils path_finder ([`bdc0a6f`](https://github.com/datax-tmp/datax-utils-deployment-helper/commit/bdc0a6f3ec106e8bc0ec3f27f71dad9383ae20bb))
* :sparkles: conf_path parameter ([`67dc189`](https://github.com/datax-tmp/datax-utils-deployment-helper/commit/67dc189e691cac476f81dbddde0d27bd46f48982))

### Documentation
* :memo: add path adjuster doc ([`777d06e`](https://github.com/datax-tmp/datax-utils-deployment-helper/commit/777d06e0f2e59dfe4ad3dc67d4602a0b5e690cf4))

## v0.4.1 (2022-12-02)
### Fix
* :bug: fix Task(ABC) to get conf_path from cli as well ([`5e42f55`](https://github.com/datax-tmp/datax-utils-deployment-helper/commit/5e42f55597540c1521be4009b53236c07795d21f))
* :bug: fix a bug in parse_auto_parameters from PipelieConfigArgumentValidators ([`0487815`](https://github.com/datax-tmp/datax-utils-deployment-helper/commit/0487815650a14ae44623b684451a40ee0a798dda))

## v0.4.0 (2022-11-30)
### Feature
* Validate pipeline cfg when pipeline cls is called directly ([`7106f0a`](https://github.com/datax-tmp/datax-utils-deployment-helper/commit/7106f0affdf1810ac330ef92e0e611355a5a77e4))

## v0.3.5 (2022-11-29)
### Fix
* :beers: rename set parameters decorators to auto parameters ([`b6b62a5`](https://github.com/datax-tmp/datax-utils-deployment-helper/commit/b6b62a5f5030344d620865dbe1f7b43a3fcc6fdc))

### Documentation
* :memo: rename tutorial section name ([`86aa9c2`](https://github.com/datax-tmp/datax-utils-deployment-helper/commit/86aa9c2c0d7ed726e6a78ef94302522540df7052))
* :memo: rename tutorial notebook in docs ([`156fc2b`](https://github.com/datax-tmp/datax-utils-deployment-helper/commit/156fc2b1714557fe09b76f4c3605cd341b00fb67))

## v0.3.4 (2022-11-25)
### Fix
* :ambulance: del __init__ from datax and utils ([`63c0341`](https://github.com/datax-tmp/datax-utils-deployment-helper/commit/63c03414681d479e4a6da40140c6c34d1d189d4b))
* **set_parameters:** :art: set_pipeline_obj to only fill missing params ([`9370bff`](https://github.com/datax-tmp/datax-utils-deployment-helper/commit/9370bffa436d22448e39181577ba838c3e311c7f))

### Documentation
* Update readme ([`c6df942`](https://github.com/datax-tmp/datax-utils-deployment-helper/commit/c6df942525fd07ea56101a7c367a537c0260a437))

## v0.3.3 (2022-11-22)
### Fix
* Pop _var_dict in pipeline only from handler ([`3ff6c8d`](https://github.com/datax-tmp/datax-utils-deployment-helper/commit/3ff6c8d1ab7b71402258b466c1c8c028a443077d))
* Del jupyter after finishing the tutorial ([`2d186d1`](https://github.com/datax-tmp/datax-utils-deployment-helper/commit/2d186d1ba9ef9aeb88c7b38cd828947eb21868e9))
* Improve ABC and set_parameters ([`ca541f6`](https://github.com/datax-tmp/datax-utils-deployment-helper/commit/ca541f606823bd64df645c569aa1cc1e4e3cac43))

## v0.3.2 (2022-11-18)
### Fix
* Add mypy ([`bb0557a`](https://github.com/datax-tmp/datax-utils-deployment-helper/commit/bb0557a6560ea00e959eeeda72364e82089573bc))
* Revert back ([`0af3cf3`](https://github.com/datax-tmp/datax-utils-deployment-helper/commit/0af3cf37290e0663ffc52a9968845d45f5233409))
* Del docs ([`1265999`](https://github.com/datax-tmp/datax-utils-deployment-helper/commit/1265999c65ad143c0e2f8f1a1675183ee44c337c))
* Del interrogate badge ([`4fd7bf3`](https://github.com/datax-tmp/datax-utils-deployment-helper/commit/4fd7bf3e9dc99109bc585cdfabd10aa704734097))
* Add mkdocs ([`3139547`](https://github.com/datax-tmp/datax-utils-deployment-helper/commit/3139547739ac5bc6c9436d466b8953bfc205a7e3))
* Del many libs from poetry ([`35f22cf`](https://github.com/datax-tmp/datax-utils-deployment-helper/commit/35f22cf829ea1f69a30a9e8706d9d8694e689671))
* Badges in readme ([`12a5f55`](https://github.com/datax-tmp/datax-utils-deployment-helper/commit/12a5f550bc7b8d65a0feff5719faaaec7e8621ee))
* Run pre-commit ([`19cd671`](https://github.com/datax-tmp/datax-utils-deployment-helper/commit/19cd6711e93e77f6c0de59e0831fa73a2214f893))
* Add interrogate and docstring ([`3757b3a`](https://github.com/datax-tmp/datax-utils-deployment-helper/commit/3757b3a5011a2e2e5609d3b067ee4ab65f9b173a))

### Documentation
* Add mkdocs resources ([`8652014`](https://github.com/datax-tmp/datax-utils-deployment-helper/commit/8652014fa0108ea791cff420e7ef6e52bd7166f8))
* Edit readme logo ([`036a66b`](https://github.com/datax-tmp/datax-utils-deployment-helper/commit/036a66b30ef9480b1434c7bf3b64aef9aafdd08e))
* Add mkdocs ([`5bfb951`](https://github.com/datax-tmp/datax-utils-deployment-helper/commit/5bfb9516fbc150ca29a06fd2caac561e1bb8575e))
* Change docstring to google format ([`aca8cd8`](https://github.com/datax-tmp/datax-utils-deployment-helper/commit/aca8cd8945b6b1a0237da2600b8168e48cfef7e9))
* Readme ([`eca17fd`](https://github.com/datax-tmp/datax-utils-deployment-helper/commit/eca17fd9758527f3f143bb9bcb211c23d0e68d3e))

## v0.3.1 (2022-11-11)
### Fix
* Use glob to find conf ([`b4ba657`](https://github.com/datax-tmp/datax-utils-deployment-helper/commit/b4ba657c8a1da7a600fc1c480dea1d824ec08cf3))

## v0.3.0 (2022-11-03)
### Feature
* Reuse Pydantic validators and add pipeline cfg validation logic ([`b80145f`](https://github.com/datax-tmp/datax-utils-deployment-helper/commit/b80145fc4d8c826535903257966b869b8769f38c))

### Fix
* Change main workflow back to previous state ([`f6b4219`](https://github.com/datax-tmp/datax-utils-deployment-helper/commit/f6b4219c1d0e403b8c2df5e172fb43fbca023750))
* **main.yml:** Change the main workflow ([`bf82687`](https://github.com/datax-tmp/datax-utils-deployment-helper/commit/bf82687a28607aecab6d8a35a6aa771cac1da659))
* Typo ([`3a9ad30`](https://github.com/datax-tmp/datax-utils-deployment-helper/commit/3a9ad308b2e67bf74f2094b828e5f11a2a101bfa))
* Fix typo ([`aa4a208`](https://github.com/datax-tmp/datax-utils-deployment-helper/commit/aa4a2086762fd2308277dd77afa24c3f0760d078))
* Add more commments ([`5955068`](https://github.com/datax-tmp/datax-utils-deployment-helper/commit/59550684b2afe5dbbc16b9e1db50fb2719a719c7))
* Run pre-commit manually ([`30edcc8`](https://github.com/datax-tmp/datax-utils-deployment-helper/commit/30edcc86a6e779674d0a67efea042d4d60c6cbdb))

## v0.2.0 (2022-11-01)
### Fix
* Pyproject and pre-commit ([`e40351c`](https://github.com/datax-tmp/datax-utils-deployment-helper/commit/e40351cd8ea87b6c326bea67fb46b03aef266b46))

## v0.1.1 (2022-10-31)
### Fix
* Fix lock file and add a dummy test ([`ba5626f`](https://github.com/datax-tmp/datax-utils-deployment-helper/commit/ba5626f2d4cc811b472a93d39a8d49e1c710dcb7))

## v0.1.0 (2022-10-31)
### Feature
* Init repo ([`7449521`](https://github.com/datax-tmp/datax-utils-deployment-helper/commit/7449521e7bcea717c90d5948f7c30f390801edbd))

### Fix
* Allow module name to be null so it's testable ([`9748c6d`](https://github.com/datax-tmp/datax-utils-deployment-helper/commit/9748c6d9cc57abd92ee184fcf2b3a3bd00f973d8))
* Add conventional-pre-commit ([`f823fc5`](https://github.com/datax-tmp/datax-utils-deployment-helper/commit/f823fc5b2d298bd1f2a8dc4c9eaf408075611405))
* Delete prints ([`cf9c75b`](https://github.com/datax-tmp/datax-utils-deployment-helper/commit/cf9c75bef88720259b145d9aa210def8e46ef1a3))
* **pre-commit:** Change hook order ([`ba9baef`](https://github.com/datax-tmp/datax-utils-deployment-helper/commit/ba9baef642ceb97edae412d9d1af31417b00e864))