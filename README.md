# Distributed key-value storage database 🔑

This project was developed for the Distributed Systems course, part of the third-year curriculum in the Software Engineering bachelor's program at the University of Minho. It focuses on implementing a key-value storage database optimized for handling concurrent operations from multiple clients.

This project obtained a final grade of **19.1/20** 💎

## 🛠️ Usage

To run the server:

```
$ ./gradlew server -Pargs=<max-clients>,<database-shards>,<user-shards>
```

To run the client:

```
$ ./gradlew client
```

To run benchmarking tests:

```
$ ./gradlew tests
```


## 🫂 Group

- **A104356** [João d'Araújo Dias Lobo](https://github.com/joaodiaslobo)
- **A90817** [Mariana Rocha Cristino](https://github.com/Mariana-rc01)
- **A100109** [Mário André Leite Rodrigues](https://github.com/MarioRodrigues10)
- **A104439** [Rita da Cunha Camacho](https://github.com/ritacamacho)
