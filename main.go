package main

import (
	"encoding/csv"
	"encoding/xml"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	inDateLayout  = "20060102150405 -0700"
	outDateLayout = "2006-01-02T15:04:05Z"
)

var (
	sourceFile   = flag.String("sourceFile", "source.xml", "the source file name")
	channelsFile = flag.String("channelsFile", "channels.csv", "the mapping file for the channels")
	outputDir    = flag.String("outputDir", ".", "output directory where result will be written")
)

type source struct {
	ChannelList []channel   `xml:"channel"`
	ProgramList []programme `xml:"programme"`
}

type title struct {
	Lang string `xml:"lang,attr"`
	Name string `xml:",chardata"`
}

func (t *title) String() string {
	return fmt.Sprintf("%s (lang=%s)", t.Name, t.Lang)
}

type channel struct {
	ID   string `xml:"id,attr"`
	Name title  `xml:"display-name"`
	URL  string `xml:"url"`
}

// <programme start="20170701080000 +0300" stop="20170701100000 +0300" channel="Alfa">
//     <title lang="bg">Добро утро, българи</title>
//   </programme>
type programme struct {
	Start         string   `xml:"start,attr"`
	Stop          string   `xml:"stop,attr"`
	ChannelName   string   `xml:"channel,attr"`
	Description   title    `xml:"desc"`
	Title         title    `xml:"title"`
	Credits       credits  `xml:"credits"`
	Date          string   `xml:"date"`
	Category      title    `xml:"category"`
	Country       []string `xml:"country"`
	EpisodeNumber string   `xml:"episode-num"`
}

type credits struct {
	Producers []string `xml:"producer"`
	Actors    []string `xml:"actor"`
}

type name struct {
	Name string `xml:",chardata"`
}

func (c *channel) String() string {
	return fmt.Sprintf("ID: %s, Name: %s, URL: %s", c.ID, c.Name.String(), c.URL)
}

type requestedChannel struct {
	ID   string
	Name string
}

type outputChannel struct {
	Name   string       `xml:"name,attr"`
	ID     string       `xml:"id,attr"`
	Events outputEvents `xml:"events"`
}

type outputEvents struct {
	Values []outputEvent `xml:"event"`
}

type outputEvent struct {
	ID                  string `xml:"id"`
	Name                string `xml:"name"`
	StartTime           string `xml:"time_from"`
	EndTime             string `xml:"time_till"`
	Description         string `xml:"description,omitempty"`
	Actors              string `xml:"actors,omitempty"`
	Directors           string `xml:"directors,omitempty"`
	ProductionYear      string `xml:"production_year,omitempty"`
	ProductionCountries string `xml:"production_countries,omitempty"`
}

func main() {
	flag.Parse()

	f, err := os.Open(*sourceFile)
	if err != nil {
		log.Fatalf("could not read source file: %v", err)
	}
	defer f.Close()

	channels := readRequestedChannels("channels.csv")

	b, err := ioutil.ReadAll(f)
	if err != nil {
		log.Fatalf("could not read source content: %v", err)
	}
	var s source
	err = xml.Unmarshal(b, &s)
	if err != nil {
		log.Fatal(err)
	}

	channelEvents := make(map[string][]programme)
	for _, e := range s.ProgramList {

		v, ok := channelEvents[e.ChannelName]
		if !ok {
			channelEvents[e.ChannelName] = []programme{e}
		} else {
			channelEvents[e.ChannelName] = append(v, e)
		}
	}

	writtenFiles := 0
	ids := make(map[string]programme)
	event_id_base := time.Now().UTC().Unix() / 1024
	for index, channel := range channels {
		events, ok := channelEvents[channel.Name]
		if !ok {
			continue
		}
		outputChannel := &outputChannel{Events: outputEvents{Values: make([]outputEvent, 0)}}
		outputChannel.ID = channel.ID
		outputChannel.Name = channel.Name
		for _, event := range events {
			startTime, err := time.Parse(inDateLayout, event.Start)
			if err != nil {
				log.Fatalf("could not parse start time due: %v", err)
			}
			endTime, err := time.Parse(inDateLayout, event.Stop)
			if err != nil {
				log.Fatalf("could not parse start time due: %v", err)
			}

			event_id_base++
			id := fmt.Sprintf("%d", event_id_base + int64(index))

			v, ok := ids[id]
			if !ok {
				ids[id] = event
			} else {
				fmt.Printf("duplication: %s - \n%v\n%v\n", id, v, event)
			}

			actors := strings.Join(event.Credits.Actors, ", ")
			directors := strings.Join(event.Credits.Producers, ", ")
			countries := strings.Join(event.Country, ", ")

			outputChannel.Events.Values = append(outputChannel.Events.Values, outputEvent{
				ID:                  id,
				Name:                event.Title.Name,
				StartTime:           startTime.UTC().Format(outDateLayout),
				EndTime:             endTime.UTC().Format(outDateLayout),
				Description:         event.Description.Name,
				Actors:              actors,
				Directors:           directors,
				ProductionYear:      event.Date,
				ProductionCountries: countries,
			})
		}

		if _, err := os.Stat(*outputDir); os.IsNotExist(err) {
			if err := os.MkdirAll(*outputDir, os.ModePerm); err != nil {
				log.Fatalf("unable to create output directory due: %v", err)
			}
		}

		outputFileName := filepath.Join(*outputDir, fmt.Sprintf("%s.xml", channel.ID))
		if err := marshalChannel(outputFileName, outputChannel); err != nil {
			log.Fatalf("could not write to output file '%s' due: %v", outputFileName, err)
		}
		writtenFiles++
	}

	log.Printf("Files written: %d\n", writtenFiles)

}

func marshalChannel(fileName string, channel *outputChannel) error {
	f, err := os.Create(fileName)
	if err != nil {

		return fmt.Errorf("unable to open output file due: %v", err)
	}
	defer f.Close()

	tmp := struct {
		outputChannel
		XMLName struct{} `xml:"channel"`
	}{outputChannel: *channel}

	enc := xml.NewEncoder(f)
	enc.Indent("  ", "    ")

	f.Write([]byte(xml.Header))

	if err := enc.Encode(tmp); err != nil {
		return fmt.Errorf("unable to marshall content due: %v", err)
	}

	return nil
}

func readRequestedChannels(fileName string) []requestedChannel {
	channelsFile, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("channels file doesn't exists")
	}
	defer channelsFile.Close()
	cr := csv.NewReader(channelsFile)

	channels, err := cr.ReadAll()
	if err != nil {
		log.Fatalf("could not read channels file due: %v", err)
	}

	result := make([]requestedChannel, 0)

	for _, rec := range channels {
		result = append(result, requestedChannel{ID: rec[0], Name: rec[1]})
	}
	return result
}
