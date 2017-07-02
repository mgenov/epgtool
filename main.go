package main

import (
	"encoding/csv"
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"
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
	Start         string  `xml:"start,attr"`
	Stop          string  `xml:"stop,attr"`
	ChannelName   string  `xml:"channel,attr"`
	Description   title   `xml:"desc"`
	Title         title   `xml:"title"`
	Credits       credits `xml:"credits"`
	Date          string  `xml:"date"`
	Category      title   `xml:"category"`
	Country       string  `xml:"country"`
	EpisodeNumber string  `xml:"episode-num"`
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

type RequestedChannel struct {
	ID   string
	Name string
}

func main() {
	f, err := os.Open("source.xml")
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

	// layout := "2006-01-02T15:04:05.000Z"
	inLayout := "20060102150405 -0700"
	outLayout := "2006-01-02T15:04:05Z"

	writtenFiles := 0
	for index, channel := range channels {
		events, ok := channelEvents[channel.Name]
		if !ok {
			continue
		}
		outputChannel := &outputChannel{Events: outputEvents{Values: make([]outputEvent, 0)}}
		outputChannel.ID = channel.ID
		outputChannel.Name = channel.Name
		for eventIndex, event := range events {
			startTime, err := time.Parse(inLayout, event.Start)
			if err != nil {
				log.Fatalf("could not parse start time due: %v", err)
			}
			endTime, err := time.Parse(inLayout, event.Stop)
			if err != nil {
				log.Fatalf("could not parse start time due: %v", err)
			}
			id := fmt.Sprintf("%d", (index+1)*1000+eventIndex)
			actors := strings.Join(event.Credits.Actors, ", ")
			directors := strings.Join(event.Credits.Producers, ", ")

			outputChannel.Events.Values = append(outputChannel.Events.Values, outputEvent{
				ID:          id,
				Name:        event.Title.Name,
				StartTime:   startTime.Format(outLayout),
				EndTime:     endTime.Format(outLayout),
				Description: event.Description.Name,
				Actors:      actors,
				Directors:   directors,
			})
		}

		outputFileName := fmt.Sprintf("n_events_%s.xml", channel.ID)
		if err := marshalChannel(outputFileName, outputChannel); err != nil {
			log.Fatalf("could not write to output file '%s' due: %v", outputFileName, err)
		}
		writtenFiles += 1
	}

	log.Printf("Written files: %d\n", writtenFiles)

}

type outputChannel struct {
	ID     string       `xml:"id,attr"`
	Name   string       `xml:"name,attr"`
	Events outputEvents `xml:"events"`
}

type outputEvents struct {
	Values []outputEvent `xml:"event"`
}

type outputEvent struct {
	ID          string `xml:"id"`
	Name        string `xml:"name"`
	Description string `xml:"description"`
	StartTime   string `xml:"time-from"`
	EndTime     string `xml:"time-till"`
	Actors      string `xml:"actors,omitempty"`
	Directors   string `xml:"directors,omitempty"`
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

func readRequestedChannels(fileName string) []RequestedChannel {
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

	result := make([]RequestedChannel, 0)

	for _, rec := range channels {
		result = append(result, RequestedChannel{ID: rec[0], Name: rec[1]})
	}
	return result
}
