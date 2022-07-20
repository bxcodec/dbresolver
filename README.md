# dbresolver
DB Connection Resolver for Cross Region Databases, Separated Readonly and ReadWrite Databases

## Idea and Inspiration

This DBResolver library will split your connections to correct defined DBs. Eg, all Read query will routed to ReadOnly replica db, and all write operation(Insert, Update, Delete) will routed to Write/Master DB. 

### Usecase 1: Separated RW and RO Database connection
<details>

<summary>Click to Expand</summary>

- You have your application deployed
- You separate the connections for optimized query 
- ![image](https://user-images.githubusercontent.com/11002383/180010864-c9e2a0b6-520d-48d6-bf0d-490eb070e75d.png) 

</details>

### Usecases 2: Cross Region Database
<details>

<summary>Click to Expand</summary>

- Your application deployed to multi regions.
- You have your Databases configured globally.
- ![image](https://user-images.githubusercontent.com/11002383/179894026-7206cbb8-35d7-4fd9-9ce9-4e62bf1ec156.png)

</details>

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
