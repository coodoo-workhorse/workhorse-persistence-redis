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

* This persistence is compatible with redis 2.8.x, 3.x.x and above* [Redis](https://redis.io/download)
* You have installed at least [Java 8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)
* You have [Maven](https://maven.apache.org/download.cgi) running on your system


## Install

1. Add the following dependency to your project ([published on Maven Central](https://search.maven.org/artifact/io.coodoo/workhorse-persistence-redis/))
   
   ```xml
   <dependency>
       <groupId>io.coodoo</groupId>
       <artifactId>workhorse-persistence-redis</artifactId>
       <version>2.0.0-RC4-SNAPSHOT</version>
   </dependency>
   ```

## Getting started

After the [installation](#install) all you need is create an `RedisPersistenceConfig` instance an pass it to the `start()` method of the `WorkhorseService`.

```java
@Inject
WorkhorseService workhorseService;

public void startWithRedisPersistence() {
    RedisPersistenceConfig redisPersistenceConfig = new RedisPersistenceConfigBuilder().build();
    workhorseService.start(redisPersistenceConfig);
}
```


## Changelog

All release changes can be viewed on our [changelog](./CHANGELOG.md).


## Maintainers

[coodoo](https://github.com/orgs/coodoo-io/people)


## Contribute

Pull requests and issues are welcome.


## License

[Apache-2.0 © coodoo GmbH](./LICENSE)

Logo: [Martin Bérubé](http://www.how-to-draw-funny-cartoons.com)
  
