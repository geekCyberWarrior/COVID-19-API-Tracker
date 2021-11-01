package main

import (
	"fmt"
	"encoding/csv"
	"bytes"
	"encoding/json"
	"strconv"
	"os"
	"github.com/joho/godotenv"

	"net/http"

	"context"
	"time"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
)

type StateName struct {
	State   string            `json:"principalSubdivision"`
}

// Response
type Response struct {
  TotalCases  int `json:"totalCases" xml:"totalCases"`
  State string `json:"state" xml:"state"`
  StateCases int `json:"stateCases" xml:"stateCases"`
  LastUpdated string `json:"lastUpdated" xml:"lastUpdated"`
}

func readCSVFromUrl(url string) ([][]string, error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	reader := csv.NewReader(resp.Body)
	reader.Comma = ','
	data, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	return data, nil
}

func writeCSVToDB(data [][]string) (error) {
	MONGODB_URL := os.Getenv("MONGODB_URL")
	fmt.Println(MONGODB_URL + "HELLO")
	client, err := mongo.NewClient(options.Client().ApplyURI(MONGODB_URL))
    if err != nil {
    	return err
    }
    ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
    err = client.Connect(ctx)
    if err != nil {
    	return err
    }
    defer client.Disconnect(ctx)

    coviddataDatabase := client.Database("coviddata")
    statewiseCollection := coviddataDatabase.Collection("statewise")


	for idx, row := range data {
		// skip header
		if idx == 0 {
			continue
		}
		if idx == 1 {
			statewiseCollection.InsertOne(ctx, bson.D{
		    {Key: "state", Value: "total"},
		    {Key: "confirmed", Value: row[1]},
			})

			statewiseCollection.InsertOne(ctx, bson.D{
		    {Key: "state", Value: "updated"},
		    {Key: "time", Value: row[5]},
			})
			continue
		}

	    statewiseCollection.InsertOne(ctx, bson.D{
		    {Key: "state", Value: row[0]},
		    {Key: "confirmed", Value: row[1]},
		})
	}

	return nil
}

func writeDataToDB() (error) {
	url := "https://data.covid19india.org/csv/latest/state_wise.csv"

	data, err := readCSVFromUrl(url)
	if err != nil {
		return err
	}

	err = writeCSVToDB(data)
	if err != nil {
		return err
	}

	return nil
}

func getStateFromCoordinates(latitude string, longitude string) (string, error) {
	params := "latitude=" + latitude + "&longitude=" + longitude
	url := fmt.Sprintf("https://api.bigdatacloud.net/data/reverse-geocode-client?%s", params)
	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}

	defer resp.Body.Close()

	var stateName StateName
	buf := new(bytes.Buffer)
	buf.ReadFrom(resp.Body)
	respByte := buf.Bytes()
	if err := json.Unmarshal(respByte, &stateName); err != nil {
		return "", err
	}

	return stateName.State, nil
}

func getConfirmedCasesFromState(state string) (int, int, string, error) {
	MONGODB_URL := os.Getenv("MONGODB_URL")
	client, err := mongo.NewClient(options.Client().ApplyURI(MONGODB_URL))
	if err != nil {
		return 0, 0, "", err
	}
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	err = client.Connect(ctx)
	if err != nil {
		return 0, 0, "", err
	}
	defer client.Disconnect(ctx)

	coviddataDatabase := client.Database("coviddata")
	statewiseCollection := coviddataDatabase.Collection("statewise")

	filterCursor, err := statewiseCollection.Find(ctx, bson.M{"state": state})
	if err != nil {
		return 0, 0, "", err
	}
	var statesFiltered []bson.M
	if err = filterCursor.All(ctx, &statesFiltered); err != nil {
		return 0, 0, "", err
	}
	confirmed, err := strconv.Atoi(statesFiltered[0]["confirmed"].(string))
	if err != nil {
		return 0, 0, "", err
	}

	filterCursor, err = statewiseCollection.Find(ctx, bson.M{"state": "total"})
	if err != nil {
		return 0, 0, "", err
	}
	var totalFiltered []bson.M
	if err = filterCursor.All(ctx, &totalFiltered); err != nil {
		return 0, 0, "", err
	}
	total, err := strconv.Atoi(totalFiltered[0]["confirmed"].(string))
	if err != nil {
		return 0, 0, "", err
	}

	filterCursor, err = statewiseCollection.Find(ctx, bson.M{"state": "updated"})
	if err != nil {
		return 0, 0, "", err
	}
	var updatedFiltered []bson.M
	if err = filterCursor.All(ctx, &updatedFiltered); err != nil {
		return 0, 0, "", err
	}
	updated := updatedFiltered[0]["time"].(string)

	return confirmed, total, updated, nil
}

func main() {
	err := godotenv.Load()
  if err != nil {
    fmt.Println("Error loading .env file")
    return
  }

	// Echo instance
	e := echo.New()

	// Middleware
	e.Use(middleware.Logger())
	e.Use(middleware.Recover())

	// Route => handler
	e.GET("/", func(c echo.Context) error {
		// Get latitude and longitude from the query string
		latitude := c.QueryParam("latitude")
		longitude := c.QueryParam("longitude")

		state, err := getStateFromCoordinates(latitude, longitude)
		if err != nil {
			return err
		}

		cases, total, updated, err := getConfirmedCasesFromState(state)
		if err != nil {
			return err
		}

		u := &Response{
			TotalCases:  total,
			State: state,
			StateCases: cases,
			LastUpdated: updated,
		}
	  return c.JSON(http.StatusOK, u)
	})

	e.POST("/writeToDB", func(c echo.Context) error {
		err := writeDataToDB()
		if err != nil {
			return err
		}

		return c.String(http.StatusOK, "Succssfully Written to DB!")
	})

	// Start server
	e.Logger.Fatal(e.Start(":1323"))
}
