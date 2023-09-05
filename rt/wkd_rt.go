// Copyright (c) 2023 Mikołaj Kuranowski
// SPDX-License-Identifier: MIT

package main

import (
	"archive/zip"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/MKuranowski/go-extra-lib/clock"
	"github.com/MKuranowski/go-extra-lib/container/bitset"
	"github.com/MKuranowski/go-extra-lib/container/set"
	"github.com/MKuranowski/go-extra-lib/encoding/mcsv"
	"github.com/MKuranowski/go-extra-lib/iter"
	"github.com/MKuranowski/go-extra-lib/resource"
	gtfsrt "github.com/MobilityData/gtfs-realtime-bindings/golang/gtfs"
	"github.com/PuerkitoBio/goquery"
	"github.com/cenkalti/backoff/v4"
	"golang.org/x/exp/maps"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
)

const (
	ColorReset  = "\x1B[0m"
	ColorYellow = "\x1B[33m"
)

type ReadAtSeeker interface {
	io.ReaderAt
	io.Seeker
}

const TimeHour = 60 * 60
const TimeDay = 24 * TimeHour

type Time int

func NewTimeFromString(x string) (Time, error) {
	colons := strings.Count(x, ":")

	var err error
	h, m, s := 0, 0, 0

	if colons == 1 {
		_, err = fmt.Sscanf(x, "%d:%d", &h, &m)
	} else if colons == 2 {
		_, err = fmt.Sscanf(x, "%d:%d:%d", &h, &m, &s)
	} else {
		err = fmt.Errorf("invalid time string: %q", x)
	}

	return Time(h*3600 + m*60 + s), err
}

func (t Time) String() string {
	s := int(t)
	m, s := s/60, s%60
	h, m := m/60, m%60
	return fmt.Sprintf("%02d:%02d:%02d", h, m, s)
}

type Date struct {
	Y int
	M time.Month
	D int
}

func NewDateFromTime(t time.Time) Date {
	y, m, d := t.Date()
	return Date{y, m, d}
}

func (d Date) AsTime(loc *time.Location) time.Time {
	return time.Date(d.Y, d.M, d.D, 0, 0, 0, 0, loc)
}

func (d Date) String() string {
	return fmt.Sprintf("%04d-%02d-%02d", d.Y, d.M, d.D)
}

type BoardEntry struct {
	TrainNumber string
	Time        Time
}

type LiveFact struct {
	StationName string
	TrainNumber string
	Time        Time
}

type MatchedFact struct {
	TripID    string
	StationID string
	Time      time.Time
}

const (
	DirectionGrodzisk = 0
	DirectionWarszawa = 1
)

/********************
 * WEBSITE SCRAPING *
 ********************/

func preProcessDocument(document *goquery.Selection) {
	// replace icons with words to be able to extract information with regular expressions
	document.Find("span.icon-train").ReplaceWithHtml("train ")
	document.Find("span.icon-clock").ReplaceWithHtml(" at ")
}

func scrapePreProcessedDocument(document *goquery.Selection) (boardsPerDirection [2]map[string][]BoardEntry, err error) {
	// Find the container rows for every train direction
	rows := document.Find("div.trains-info-content div.row")

	// Check if we have enough containers
	if rows.Length() < 2 {
		err = fmt.Errorf("not enough rows in \"div.trains-info-content\" - got %d, expected at least 2", rows.Length())
		return
	}

	// Iterate over every row and parse realtime departures from it,
	// but only if that row actually represents a line direction.
	rows.EachWithBreak(func(i int, row *goquery.Selection) bool {
		// Get this row's direction
		directionText := row.Find(".stations-direction-info > strong").First().Text()
		var direction int
		switch directionText {
		case "GRODZISK MAZ.":
			direction = DirectionGrodzisk
		case "WARSZAWA":
			direction = DirectionWarszawa
		case "":
			return true // Skip rows without a direction
		default:
			err = fmt.Errorf("unknown direction in trains-info-content's row %d: %q", i, directionText)
			return false
		}

		// Ensure directions are not duplicated
		if boardsPerDirection[direction] != nil {
			err = fmt.Errorf("duplicate trains-info-content rows in direction %q", directionText)
			return false
		}

		// Parse the departure boards
		var boards map[string][]BoardEntry
		boards, err = scrapeDirectionList(row)
		if err != nil {
			err = fmt.Errorf("parse row %d in direction %s: %w", i, directionText, err)
			return false
		}

		boardsPerDirection[direction] = boards
		return true
	})

	return
}

func scrapeDirectionList(trainsInfoContentRow *goquery.Selection) (boards map[string][]BoardEntry, err error) {
	// Find all the list elements representing stations
	stationElements := trainsInfoContentRow.Find(".stations-route > li.point")

	// Check that we have enough station list elements
	if stationElements.Length() == 0 {
		err = fmt.Errorf("no station elements (li.point) within stations-route list")
		return
	}

	boards = make(map[string][]BoardEntry)

	// Iterate over every station and parse their departure boards
	stationElements.EachWithBreak(func(i int, stationElement *goquery.Selection) bool {
		// Extract the station name and map it to GTFS stop_Id
		stationName := strings.TrimSpace(strings.SplitN(stationElement.AttrOr("title", ""), "-", 2)[0])

		// Ensure that stations are unique within this direction
		if _, duplicate := boards[stationName]; duplicate {
			err = fmt.Errorf("element %d (%s): duplicate entry for this station", i, stationName)
			return false
		}

		// Find the list with all departures
		details := stationElement.Find(".station-timetable-details")
		if details.Length() == 0 {
			err = fmt.Errorf("element %d (%s): missing departure list (.station-timetable-details)", i, stationName)
			return false
		}

		// Parse the departure list
		var board []BoardEntry
		board, err = scrapeStationTimetableDetails(details)

		if err != nil {
			err = fmt.Errorf("element %d (%s): %w", i, stationName, err)
			return false
		}

		boards[stationName] = board
		return true
	})

	return
}

var timetableEntryPattern = regexp.MustCompile(`train\s+nr\s+(\w+)\s+at\s+(\d?\d:\d\d)`)

func scrapeStationTimetableDetails(stationTimetableDetails *goquery.Selection) (board []BoardEntry, err error) {
	// NOTE: No checking if there are no <li> elements in stationTimetableDetails;
	//       missing departures are expected e.g. at night, when no trains are running.
	stationTimetableDetails.Find("li").EachWithBreak(func(i int, entry *goquery.Selection) bool {
		// Try to match the entry content (text) against a known regular expression.
		// The preProcessDocument should have replaced icons used on the website with
		// descriptive strings, expected by the `timetableEntryPattern`.
		entryText := entry.Text()
		match := timetableEntryPattern.FindStringSubmatch(entryText)

		if len(match) == 0 {
			err = fmt.Errorf("timetable entry %d: unrecognized string %q", i, entryText)
			return false
		}

		number := strings.TrimLeft(match[1], "0")

		// Try to parse the time value from the match
		var time Time
		time, err = NewTimeFromString(match[2])
		if err != nil {
			err = fmt.Errorf("timetable entry %d: %w", i, err)
			return false
		}

		board = append(board, BoardEntry{TrainNumber: number, Time: time})
		return true
	})
	return
}

func scrapeDocument(document *goquery.Selection) (boardsPerDirection [2]map[string][]BoardEntry, err error) {
	preProcessDocument(document)
	return scrapePreProcessedDocument(document)
}

func GetLiveFacts() ([]LiveFact, error) {
	// Try to fetch the website
	resp, err := http.Get("https://wkd.com.pl/?tmpl=module&module_id=123")
	if err != nil {
		return nil, fmt.Errorf("failed to request live website: %w", err)
	}
	defer resp.Body.Close()

	// Fail on non-success responses
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("failed to request live website: %s", resp.Status)
	}

	// Parse the website
	doc, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to parse live website: %w", err)
	}

	// Scrape the website
	boardsPerDirection, err := scrapeDocument(doc.Selection)
	if err != nil {
		return nil, fmt.Errorf("failed to scrape live website: %w", err)
	}

	// Pull all the data into live facts
	facts := make([]LiveFact, 0)
	for _, boards := range boardsPerDirection {
		for stationName, board := range boards {
			for _, departure := range board {
				facts = append(facts, LiveFact{
					StationName: stationName,
					TrainNumber: departure.TrainNumber,
					Time:        departure.Time,
				})
			}
		}
	}

	return facts, nil
}

/**********************
 * GTFS DATA HANDLING *
 **********************/

const MidnightOffset = 3 * time.Hour

type StaticData struct {
	GTFSResource resource.Interface

	// StopNameToID maps stop_name to stop_id from the stops.txt file
	StopNameToID map[string]string

	// DateToServices maps a day to all service_ids running on that date
	DateToServices map[Date]set.Set[string]

	// ServiceIDToNumberToTripID maps a service_id to a TrainNumberToTripID mapping
	ServiceIDToNumberToTripID map[string]map[string]string

	// TrainNumberToTripID maps trip_short_name (train number) to the trip_id;
	// valid only until the end of a day; see Until.
	TrainNumberToTripID map[string]string

	// Until indicates the validity of the TrainNumberToTripID mapping.
	Until time.Time

	// Clock is used by the StaticData to tell the current time.
	// If nil, clock.System is used.
	Clock clock.Interface

	warsawTime *time.Location
}

func (sd *StaticData) lazyInit() {
	if sd.Clock == nil {
		sd.Clock = clock.System
	}

	if sd.warsawTime == nil {
		var err error
		sd.warsawTime, err = time.LoadLocation("Europe/Warsaw")
		if err != nil {
			panic(fmt.Sprintf("failed to load Europe/Warsaw timezone: %v", err))
		}
	}
}

func (sd *StaticData) refreshTrainNumbers() error {
	now := sd.Clock.Now().In(sd.warsawTime)

	// Figure out the active services
	serviceDay := NewDateFromTime(now.Add(-MidnightOffset))
	services := sd.DateToServices[serviceDay]

	// Check the amount of services
	if len(services) == 0 {
		return fmt.Errorf("no services valid at %s", serviceDay)
	}
	log.Println("Refreshing train number mapping - services", services, "will be used on", serviceDay)

	// Update the TrainNumberToTripID mapping and
	sd.TrainNumberToTripID = make(map[string]string)
	for serviceID := range services {
		// FIXME: check for conflicts
		maps.Copy(sd.TrainNumberToTripID, sd.ServiceIDToNumberToTripID[serviceID])
	}

	// Update the validity period - 3:00 on the next day
	sd.Until = serviceDay.AsTime(sd.warsawTime).AddDate(0, 0, 1).Add(MidnightOffset)

	return nil
}

func (sd *StaticData) readStops(gtfs fs.FS) error {
	// Reset the StopNameToID table
	sd.StopNameToID = make(map[string]string)

	// Try to open stops.txt
	f, err := gtfs.Open("stops.txt")
	if err != nil {
		return fmt.Errorf("stops.txt: Open: %w", err)
	}
	defer f.Close()
	log.Print("Loading stops.txt")

	// Read the CSV file
	r := mcsv.NewReader(f)
	r.ReuseRecord = true
	for {
		// Try to get the next record
		record, err := r.Read()
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return fmt.Errorf("stops.txt: Read: %w", err)
		}

		// Get its stop_id
		id, has := record["stop_id"]
		if !has {
			return fmt.Errorf("stops.txt: missing stop_id")
		}

		// Get its stop_name
		name, has := record["stop_name"]
		if !has {
			return fmt.Errorf("stops.txt: missing stop_name")
		}

		// Update the table
		// FIXME: check for duplicates
		sd.StopNameToID[name] = id
	}

	return nil
}

func calendarRecordToActiveWeekdays(record map[string]string) (s bitset.Small) {
	if record["monday"] == "1" {
		s.Add(int(time.Monday))
	}
	if record["tuesday"] == "1" {
		s.Add(int(time.Tuesday))
	}
	if record["wednesday"] == "1" {
		s.Add(int(time.Wednesday))
	}
	if record["thursday"] == "1" {
		s.Add(int(time.Thursday))
	}
	if record["friday"] == "1" {
		s.Add(int(time.Friday))
	}
	if record["saturday"] == "1" {
		s.Add(int(time.Saturday))
	}
	if record["sunday"] == "1" {
		s.Add(int(time.Sunday))
	}
	return
}

func (sd *StaticData) readCalendar(gtfs fs.FS) error {
	// Clear the DateToServices table
	sd.DateToServices = make(map[Date]set.Set[string])

	// Try to open calendar.txt
	f, err := gtfs.Open("calendar.txt")
	if errors.Is(err, fs.ErrNotExist) {
		log.Print("calendar.txt does not exist")
		return nil
	} else if err != nil {
		return fmt.Errorf("calendar.txt: Open: %w", err)
	}
	defer f.Close()
	log.Print("Loading calendar.txt")

	// Read the CSV file
	r := mcsv.NewReader(f)
	r.ReuseRecord = true
	for {
		// Get next record from calendar.txt
		record, err := r.Read()
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return fmt.Errorf("calendar.txt: Read: %w", err)
		}

		// Get its service_id
		id, has := record["service_id"]
		if !has {
			return fmt.Errorf("calendar.txt: missing service_id")
		}

		// Get the start_date
		day, err := time.ParseInLocation("20060102", record["start_date"], sd.warsawTime)
		if err != nil {
			return fmt.Errorf("calendar.txt: invalid start_date: %w", err)
		}

		// Get the end_date
		end, err := time.ParseInLocation("20060102", record["end_date"], sd.warsawTime)
		if err != nil {
			return fmt.Errorf("calendar.txt: invalid end_date: %w", err)
		}

		// Get the set of valid weekdays
		validWeekdays := calendarRecordToActiveWeekdays(record)

		// Iterate over the calendar span
		for !day.After(end) {
			// Check if day is active
			if validWeekdays.Has(int(day.Weekday())) {
				date := NewDateFromTime(day)

				servicesSet, has := sd.DateToServices[date]
				if !has {
					sd.DateToServices[date] = set.Set[string]{id: {}}
				} else {
					servicesSet.Add(id)
				}
			}

			day = day.AddDate(0, 0, 1)
		}
	}

	return nil
}

func (sd *StaticData) readCalendarDates(gtfs fs.FS) error {
	// Try to open calendar_dates.txt
	f, err := gtfs.Open("calendar_dates.txt")
	if errors.Is(err, fs.ErrNotExist) {
		log.Print("calendar_dates.txt does not exist")
		return nil
	} else if err != nil {
		return fmt.Errorf("calendar_dates.txt: Open: %w", err)
	}
	defer f.Close()
	log.Print("Loading calendar_dates.txt")

	// Read the CSV file
	r := mcsv.NewReader(f)
	r.ReuseRecord = true
	for {
		// Get next record from calendar.txt
		record, err := r.Read()
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return fmt.Errorf("calendar_dates.txt: Read: %w", err)
		}

		// Get its service_id
		id, has := record["service_id"]
		if !has {
			return fmt.Errorf("calendar_dates.txt: missing service_id")
		}

		// Get the date
		day, err := time.ParseInLocation("20060102", record["date"], sd.warsawTime)
		if err != nil {
			return fmt.Errorf("calendar.txt: invalid date: %w", err)
		}
		date := NewDateFromTime(day)

		// Update the DateToServices table
		et := record["exception_type"]
		switch et {
		case "1":
			servicesSet, ok := sd.DateToServices[date]
			if ok {
				servicesSet.Add(id)
			} else {
				sd.DateToServices[date] = set.Set[string]{id: {}}
			}

		case "2":
			servicesSet, ok := sd.DateToServices[date]
			if ok {
				servicesSet.Remove(id)
			}

		case "":
			return fmt.Errorf("calendar_dates.txt: missing exception_type")

		default:
			return fmt.Errorf("calendar.txt: invalid exception_type: %q", et)
		}
	}

	return nil
}

func (sd *StaticData) readTrips(gtfs fs.FS) error {
	// Clear the ServiceIDToNumberToTripID table
	sd.ServiceIDToNumberToTripID = make(map[string]map[string]string)

	// Try to open trips.txt
	f, err := gtfs.Open("trips.txt")
	if err != nil {
		return fmt.Errorf("trips.txt: Open: %w", err)
	}
	defer f.Close()
	log.Print("Loading trips.txt")

	// Read the CSV file
	r := mcsv.NewReader(f)
	r.ReuseRecord = true
	for {
		// Try to get the next record
		record, err := r.Read()
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return fmt.Errorf("trips.txt: Read: %w", err)
		}

		// Get its trip_id
		id, ok := record["trip_id"]
		if !ok {
			return fmt.Errorf("calendar.txt: missing trip_id")
		}

		// Get its service_id
		serviceID, ok := record["service_id"]
		if !ok {
			return fmt.Errorf("calendar.txt: missing service_id")
		}

		// Get its train number
		number, ok := record["trip_short_name"]
		if !ok {
			return fmt.Errorf("calendar.txt: missing trip_short_name")
		}

		// Update the ServiceIDToNumberToTripID table
		numberToTripTable, ok := sd.ServiceIDToNumberToTripID[serviceID]
		if ok {
			numberToTripTable[number] = id
		} else {
			sd.ServiceIDToNumberToTripID[serviceID] = map[string]string{number: id}
		}
	}

	return nil
}

func (sd *StaticData) refreshGTFS(gtfs fs.FS) error {
	err := sd.readStops(gtfs)
	if err != nil {
		return err
	}

	err = sd.readCalendar(gtfs)
	if err != nil {
		return err
	}

	err = sd.readCalendarDates(gtfs)
	if err != nil {
		return err
	}

	err = sd.readTrips(gtfs)
	if err != nil {
		return err
	}

	return nil
}

func (sd *StaticData) Refresh() error {
	// Initialize the state
	sd.lazyInit()

	// Check if the train numbers need to be refreshed.
	// This flag will also be set if the GTFS has changed.
	refreshTrainNumbers := sd.Clock.Now().After(sd.Until)

	// Fetch the data
	body, _, err := sd.GTFSResource.Fetch(resource.Conditional)
	if err != nil {
		return fmt.Errorf("GTFSResource.Fetch: %w", err)
	}

	// Refresh the GTFS if it has changed
	if body != nil {
		refreshTrainNumbers = true
		log.Print("GTFS changed - updating")

		// Ensure the resource implements Seek and ReadAt
		file, ok := body.(ReadAtSeeker)
		if !ok {
			panic(fmt.Sprintf("GTFSResource.Fetch: got %T, which does not implement io.ReaderAt and/or io.Seeker", body))
		}

		// Try to tell the size of the file
		size, err := file.Seek(0, io.SeekEnd)
		if err != nil {
			return fmt.Errorf("GTFSResource.Seek: %w", err)
		}

		// Open the ZIP file
		gtfs, err := zip.NewReader(file, size)
		if err != nil {
			return fmt.Errorf("zip.NewReader(GTFSResource): %w", err)
		}

		err = sd.refreshGTFS(gtfs)
		if err != nil {
			return err
		}
	}

	// Update train number mappings if necessary
	if refreshTrainNumbers {
		err = sd.refreshTrainNumbers()
		if err != nil {
			return err
		}
	}

	return nil
}

func (sd *StaticData) MatchWith(facts []LiveFact, fetchTime time.Time) ([]MatchedFact, error) {
	// Ensure fetchTime is in Warsaw timezone
	serviceDay := NewDateFromTime(fetchTime.In(sd.warsawTime).Add(-MidnightOffset))

	// Prepare a slice for matched facts
	matched := make([]MatchedFact, 0, len(facts))

	// Match facts
	for _, liveFact := range facts {
		var matchedFact MatchedFact
		var ok bool

		// Try to match the station
		// Consider mismatches as errors - these should be static,
		// and if they stop matching - either the GTFS needs to be updated,
		// or the scraped website started doing something different.
		matchedFact.StationID, ok = sd.StopNameToID[liveFact.StationName]
		if !ok {
			return nil, fmt.Errorf("unknown station name: %q", liveFact.StationName)
		}

		// Try to match the number to a trip ID
		matchedFact.TripID, ok = sd.TrainNumberToTripID[liveFact.TrainNumber]
		if !ok {
			log.Print(ColorYellow, "Unknown train number:", liveFact.TrainNumber, ColorReset)
			continue
		}

		// Normalize the departure time
		// Not sure whether the website uses 24:xx or 00:xx for trips which past-midnight
		// (probably the latter), but try to match both.
		dayOffset := 0
		normalizedTime := int(liveFact.Time)

		if normalizedTime > TimeDay {
			dayOffset, normalizedTime = normalizedTime/TimeDay, normalizedTime%TimeDay
		} else if normalizedTime < 3*TimeHour {
			dayOffset = 1
		}

		// Convert to hours, minutes and seconds
		m, s := normalizedTime/60, normalizedTime%60
		h, m := m/60, m%60

		// Create a time.Time from all the extracted info
		matchedFact.Time = time.Date(serviceDay.Y, serviceDay.M, serviceDay.D, h, m, s, 0, sd.warsawTime).AddDate(0, 0, dayOffset)
		matched = append(matched, matchedFact)
	}

	return matched, nil
}

/**************************
 * GTFS-Realtime creation *
 **************************/

// Ptr returns a pointer to v. Useful for constants and literals.
func Ptr[T any](v T) *T { return &v }

func ToGTFSRealtime(facts []MatchedFact, updateTime time.Time) *gtfsrt.FeedMessage {
	// Group all facts by tripID
	factsByTrip := iter.AggregateBy(iter.OverSlice(facts), func(fact MatchedFact) string { return fact.TripID })

	// Create all entities
	entities := make([]*gtfsrt.FeedEntity, 0, len(factsByTrip))
	for tripID, facts := range factsByTrip {
		tripID := tripID

		// Create stop_time_updates for this trip's facts
		updates := make([]*gtfsrt.TripUpdate_StopTimeUpdate, 0, len(facts))
		for _, fact := range facts {
			updates = append(updates, &gtfsrt.TripUpdate_StopTimeUpdate{
				StopId:    Ptr(fact.StationID),
				Departure: &gtfsrt.TripUpdate_StopTimeEvent{Time: Ptr(fact.Time.Unix())},
			})
		}

		// Add a FeedEntity with this trip's updates to the feed
		entities = append(entities, &gtfsrt.FeedEntity{
			Id: Ptr(tripID),
			TripUpdate: &gtfsrt.TripUpdate{
				Trip:           &gtfsrt.TripDescriptor{TripId: Ptr(tripID)},
				StopTimeUpdate: updates,
			},
		})
	}

	// Return a GTFS-RT FeedMessage
	return &gtfsrt.FeedMessage{
		Header: &gtfsrt.FeedHeader{
			GtfsRealtimeVersion: Ptr("2.0"),
			Incrementality:      Ptr(gtfsrt.FeedHeader_FULL_DATASET),
			Timestamp:           Ptr(uint64(updateTime.Unix())),
		},
		Entity: entities,
	}
}

func SaveProtoToFile(m proto.Message, target string, humanReadable bool) (err error) {
	// Try to marshall the data
	var data []byte
	if humanReadable {
		data, err = prototext.Marshal(m)
	} else {
		data, err = proto.Marshal(m)
	}

	if err != nil {
		err = fmt.Errorf("failed to marshal to protobuf: %w", err)
		return
	}

	// Save data to a temporary file
	tempFile := target + ".tmp"
	err = os.WriteFile(tempFile, data, 0o666)
	if err != nil {
		err = fmt.Errorf("failed to write to %s: %w", tempFile, err)
		return
	}

	// Rename the temporary file to the target file
	err = os.Rename(tempFile, target)
	if err != nil {
		err = fmt.Errorf("failed to move %s to %s: %w", tempFile, target, err)
		return
	}

	return
}

/*********************
 * Main entry points *
 *********************/

func oneShot(sd *StaticData, target string, humanReadable bool) (err error) {
	sd.Refresh()

	log.Print("Requesting live data")
	fetchTime := sd.Clock.Now()
	live, err := GetLiveFacts()
	if err != nil {
		return
	}

	log.Print("Matching with static data")
	matched, err := sd.MatchWith(live, fetchTime)
	if err != nil {
		return
	}

	log.Print("Matched ", len(matched), " facts; ", len(live)-len(matched), " remained unmatched")

	log.Print("Saving to target file")
	err = SaveProtoToFile(ToGTFSRealtime(matched, fetchTime), target, humanReadable)
	return
}

func loop(period time.Duration, sd *StaticData, target string, humanReadable bool) {
	// Create a backoff object for exponential back-off on WKD website errors
	b := &backoff.ExponentialBackOff{
		InitialInterval:     10 * time.Second,
		RandomizationFactor: 0.2,
		Multiplier:          2,
		MaxInterval:         24 * time.Hour,
		MaxElapsedTime:      24 * time.Hour,
		Stop:                backoff.Stop,
		Clock:               sd.Clock,
	}

	for {
		// Reset the backoff state
		b.Reset()

		// Try to get the latest live facts.
		// Backoff when scraping fails.
		var fetchTime time.Time
		live, err := backoff.RetryNotifyWithData(
			func() (live []LiveFact, err error) {
				// Try to refresh the static data
				err = sd.Refresh()
				if err != nil {
					return nil, backoff.Permanent(err)
				}

				// Try to get the live data
				fetchTime = sd.Clock.Now()
				live, err = GetLiveFacts()
				return
			},
			b,
			func(err error, d time.Duration) {
				log.Printf("Backing off until %s - updating failed: %s", d, err)
			},
		)
		if err != nil {
			log.Fatal(err)
		}

		// Match live facts with static data
		matched, err := sd.MatchWith(live, fetchTime)
		if err != nil {
			log.Fatal("Matching:", err)
		}

		// Save to the target file
		err = SaveProtoToFile(ToGTFSRealtime(matched, fetchTime), target, humanReadable)
		if err != nil {
			log.Fatal(err)
		}

		log.Print("GTFS-RT updated. Matched ", len(matched), " facts; ", len(live)-len(matched), " remained unmatched.")

		// Sleep until next update
		time.Sleep(period)
	}
}

func main() {
	// CLI Flags
	flagTarget := flag.String("target", "./wkd.pb", "where to put the GTFS-RT file")
	flagHumanReadable := flag.Bool("human-readable", false, "use human-readable protobuf format")
	flagLoop := flag.Duration("loop", time.Duration(0), "if non-zero, how often to update the target file; otherwise the script runs only once")
	flagGtfsPath := flag.String("gtfs-path", "./wkd.zip", "path to the static GTFS file")
	flag.Parse()

	// Prepare a static data resource
	sd := &StaticData{
		GTFSResource: &resource.TimeLimited{
			R:                  resource.Local(*flagGtfsPath),
			MinimalTimeBetween: 5 * time.Minute,
		},
		Clock: clock.System,
	}

	if *flagLoop != 0 {
		loop(*flagLoop, sd, *flagTarget, *flagHumanReadable)
	} else {
		err := oneShot(sd, *flagTarget, *flagHumanReadable)
		if err != nil {
			log.Fatal(err)
		}
	}
}
