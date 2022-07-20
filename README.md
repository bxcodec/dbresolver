# dbresolver
DB Connection Resolver for Cross Region Databases, Separated Readonly and ReadWrite Databases

## Ideas and Inspiration

![image](https://user-images.githubusercontent.com/11002383/179894026-7206cbb8-35d7-4fd9-9ce9-4e62bf1ec156.png)

This DBResolver library will split your connections to correct defined DBs. Eg, All Select query will routed to ReadOnly replica db, and all write operation(Insert, Update, Delete) will routed to Write/Master DB.

## Support

You can file an [Issue](https://github.com/bxcodec/dbresolver/issues/new).
See documentation in [Go.Dev](https://pkg.go.dev/github.com/bxcodec/dbresolver?tab=doc)

## Getting Started

#### Download

```shell
go get -u github.com/bxcodec/dbresolver
```

# Example
---
(TODO) bxcodec


## Contribution
---

To contrib to this project, you can open a PR or an issue.
