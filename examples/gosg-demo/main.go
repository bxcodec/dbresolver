package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/bxcodec/dbresolver/v2"
	"github.com/labstack/echo/v4"

	"github.com/Masterminds/squirrel"
	_ "github.com/lib/pq"
)

func initDBResolver() dbresolver.DB {
	var (
		rwHost     = "localhost"
		rwPort     = 5432
		rwUser     = "postgres"
		rwPassword = "my_password"
		roHost     = "localhost"
		roPort     = 5433
		roUser     = "postgres"
		roPassword = "my_password"
		dbname     = "my_database"
	)
	// connection string
	rwPrimary := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", rwHost, rwPort, rwUser, rwPassword, dbname)
	readOnlyReplica := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", roHost, roPort, roUser, roPassword, dbname)

	// open database for primary
	dbPrimary, err := sql.Open("postgres", rwPrimary)
	if err != nil {
		log.Fatal("go error when connecting to the RW DB")
	}
	// configure the DBs for other setup eg, tracing, etc
	// eg, tracing.Postgres(dbPrimary)

	// open database for replica
	dbReadOnlyReplica, err := sql.Open("postgres", readOnlyReplica)
	if err != nil {
		log.Fatal("go error when connecting to the RO DB")
	}
	// configure the DBs for other setup eg, tracing, etc
	// eg, tracing.Postgres(dbReadOnlyReplica)

	connectionDB := dbresolver.New(
		dbresolver.WithPrimaryDBs(dbPrimary),
		dbresolver.WithReplicaDBs(dbReadOnlyReplica),
		dbresolver.WithLoadBalancer(dbresolver.RoundRobinLB))
	return connectionDB
}

func main() {
	connectionDB := initDBResolver()
	defer connectionDB.Close()
	// now you can use the connection for all DB operation
	insertedIDs := insertMasterData(connectionDB)
	res := queryArticles(connectionDB, insertedIDs)
	fmt.Println("Queried Articles: ", res)

	stmt, err := connectionDB.Prepare("SELECT article_id, title, content FROM articles WHERE article_id = $1")
	if err != nil {
		log.Print("failed to prepare the query", err)
	}
	defer stmt.Close()

	e := echo.New()
	e.GET("/", func(c echo.Context) error {
		res := queryArticles(connectionDB, insertedIDs)
		fmt.Println("Queried Articles: ", res)

		res = queryArticlesWithoutPrepare(connectionDB, insertedIDs)
		fmt.Println("Queried Articles Without Prepare ", res)

		id := c.QueryParam("id")
		singleArticle := queryRow(connectionDB, id)
		fmt.Println("Queried Single Article: ", singleArticle)

		singleArticlePrepared := queryRowPrepare(connectionDB, id)
		fmt.Println("Queried Single Article: ", singleArticlePrepared)

		singleArticlePreparedStmt := queryRowPreparedStmt(stmt, id)
		fmt.Println("Queried Single Article with Prepared Stmt: ", singleArticlePreparedStmt)

		return c.String(http.StatusOK, "Hello, World!")
	})
	e.Logger.Fatal(e.Start(":1323"))
}

func insertMasterData(db dbresolver.DB) []string {
	articles := []Article{
		{
			Title:   "Lorem Ipsum",
			Content: "Dolor Sit Amet",
		},
	}

	// we're using transaction here from app-level
	// to tell the library to use RW connection
	// disabling this will raise issue: " pq: cannot execute INSERT in a read-only transaction"
	tx, errTx := db.Begin()
	if errTx != nil {
		log.Fatal("failed to begin ", errTx)
	}
	stmt, err := tx.PrepareContext(context.Background(),
		"INSERT INTO articles (title, content, created_time) values ($1, $2, $3) RETURNING article_id;")
	if err != nil {
		log.Fatal("failed to insert master data ", err)
	}
	defer stmt.Close()
	articleIds := []string{}
	for index, article := range articles {

		row := stmt.QueryRow(article.Title, article.Content, time.Now())
		var id int64
		err = row.Scan(&id)
		if err != nil {
			log.Println("failed to insert new article, ", err)
		}
		idStr := fmt.Sprintf("%d", id)
		articleIds = append(articleIds, idStr)
		articles[index].ID = idStr
	}
	tx.Commit()
	fmt.Println("Inserted Articles ", articles)
	return articleIds
}

func queryArticles(db dbresolver.DB, articleIDs []string) []Article {
	stmt, err := db.Prepare("SELECT article_id, title, content FROM articles WHERE article_id IN($1)")
	if err != nil {
		log.Print("failed to prepare the query", err)
	}
	defer stmt.Close()
	rows, err := stmt.Query(strings.Join(articleIDs, ","))
	if err != nil {
		log.Print("failed to query using IDs", err)
	}

	res := []Article{}
	for rows.Next() {
		var article Article
		var articleID int64
		errScan := rows.Scan(&articleID, &article.Title, &article.Content)
		if errScan != nil {
			log.Print("failed to scan rows, ", errScan)
		}

		article.ID = fmt.Sprintf("%d", articleID)
		res = append(res, article)
	}
	return res
}

func queryRowPrepare(db dbresolver.DB, articleID string) Article {
	stmt, err := db.Prepare("SELECT article_id, title, content FROM articles WHERE article_id = $1")
	if err != nil {
		log.Print("failed to prepare the query", err)
	}
	defer stmt.Close()
	row := stmt.QueryRow(articleID)
	var article Article
	var dbArticleID int64
	errScan := row.Scan(&articleID, &article.Title, &article.Content)
	if errScan != nil {
		log.Print("failed to scan rows, ", errScan)
	}
	article.ID = fmt.Sprintf("%d", dbArticleID)
	return article
}

func queryRowPreparedStmt(stmt dbresolver.Stmt, articleID string) Article {
	row := stmt.QueryRow(articleID)
	var article Article
	var dbArticleID int64
	errScan := row.Scan(&articleID, &article.Title, &article.Content)
	if errScan != nil {
		log.Print("failed to scan rows, ", errScan)
	}
	article.ID = fmt.Sprintf("%d", dbArticleID)
	return article
}

func queryRow(db dbresolver.DB, articleID string) Article {
	sql, args, err := squirrel.Select("article_id", "title", "content").
		From("articles").PlaceholderFormat(squirrel.Dollar).
		Where(squirrel.Eq{"article_id": articleID}).ToSql()
	if err != nil {
		log.Print("failed to build the query", err)
	}
	row := db.QueryRow(sql, args...)

	var article Article
	var dbArticleID int64
	errScan := row.Scan(&articleID, &article.Title, &article.Content)
	if errScan != nil {
		log.Print("failed to scan rows, ", errScan)
	}
	article.ID = fmt.Sprintf("%d", dbArticleID)
	return article
}

func queryArticlesWithoutPrepare(db dbresolver.DB, articleIDs []string) []Article {
	sql, args, err := squirrel.Select("article_id", "title", "content").
		From("articles").PlaceholderFormat(squirrel.Dollar).
		Where(squirrel.Eq{"article_id": articleIDs}).ToSql()
	if err != nil {
		log.Print("failed to build the query", err)
	}
	rows, err := db.Query(sql, args...)
	if err != nil {
		log.Print("failed to run the query: ", sql, err)
	}
	res := []Article{}
	for rows.Next() {
		var article Article
		var articleID int64
		errScan := rows.Scan(&articleID, &article.Title, &article.Content)
		if errScan != nil {
			log.Print("failed to scan rows, ", errScan)
		}

		article.ID = fmt.Sprintf("%d", articleID)
		res = append(res, article)
	}
	return res
}

type Article struct {
	ID          string
	Title       string
	Content     string
	CreatedTime string
}
