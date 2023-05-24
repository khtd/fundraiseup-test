# fundraiseup backend test by Agranov Sonya

## How to build

install dependencies

```
$ npm run build
```

build

```
$ npm run build
```

## How to run

there are two parts of the application

### generator (app.ts)

in dev mode you can run it by

```
$ npm run app:dev
```

in prod

```
$ node ./dist/app.js
```

difference between dev and prod mode - i assume, that you do not want to use .env files on prod.

### sync app (sync.ts)

similarly

```
$ npm run sync:dev
```

or

```
$ node ./dist/sync.js
```

additionally, there is a "full-reindex" mode

```
$ npm run sync:dev:reindex
```

or

```
$ node ./dist/sync.js --full-reindex
```
