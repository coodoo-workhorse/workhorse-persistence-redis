# Workhorse Persistence for Redis

> Redis Workhorse-Persistence Implementation

## Table of Contents
<img align="right" height="200px" src="logo.png">

- [Prerequisites](#prerequisites)
- [Get Workhorse](#get-workhorse)
- [Install](#install)
- [Getting started](#getting-started)
- [Maintainers](#maintainers)
- [Changelog](#changelog)
- [Contribute](#contribute)
- [License](#license)
  

## Prerequisites

Before you begin, ensure you have met the following requirements:
* You have installed at least [Java 8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)
* You have [Maven](https://maven.apache.org/download.cgi) running on your system
  
## Get Workhorse
  
Please create a [gitLab token](https://gitlab.coodoo.io/profile/personal_access_tokens) and store it in the settings.xml of your local maven repository as follows:

```
<server>
	<id>gitlab-maven</id>
		<configuration>
			<httpHeaders>
				<property>
					<name>Private-Token</name>
					<value>ENTER_YOUR_PRIVATE_TOKEN_HERE</value>
				</property>
		</httpHeaders>
	</configuration>
</server>
```

Run command :

```
mvn dependency:get -Dartifact=io.coodoo:workhorse:2.0.0-RC1-SNAPSHOT
```

## Install

TODO


## Getting started

TODO


## Changelog

All release changes can be viewed on our [changelog](./CHANGELOG.md).


## Maintainers

[coodoo](https://github.com/orgs/coodoo-io/people)


## Contribute

Pull requests and issues are welcome.


## License

[Apache-2.0 © coodoo GmbH](./LICENSE)

Logo: [Martin Bérubé](http://www.how-to-draw-funny-cartoons.com)
  
