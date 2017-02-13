package collector

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/smartystreets/goconvey/convey"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
)

func TestScrapeCFYIntances(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("error opening a stub database connection: %s", err)
	}
	defer db.Close()

	columns := []string{"name"}
	rows := sqlmock.NewRows(columns).
		AddRow("service_instances")
	mock.ExpectQuery(sanitizeQuery("SHOW TABLE IN `mysql_broker` where name='service_instances'")).WillReturnRows(rows)

	columns = []string{"plan_guid", "db_name", "max_storage_mb"}
	rows = sqlmock.NewRows(columns).
		AddRow("11d0aa36-dcec-4021-85f5-ea4d9a5c8342", "cf_908c8fc1_5103_42df_a596_32b25950482a", "1048576000")
	rows = rows.AddRow("ab08f1bc-e6fc-4b56-a767-ee0fea6e3f20", "cf_af1ecf20_2392_4c62_b793_86426e6b897c", "104857600")
	rows = rows.AddRow("ab08f1bc-e6fc-4b56-a767-ee0fea6e3f20", "cf_6588904f_0372_4b62_8e62_821c966d95dd", "262144000")
	mock.ExpectQuery(sanitizeQuery("select plan_guid, db_name, `max_storage_mb`*1024*1024 as max_storage_mb from `mysql_broker`.`service_instances`")).WillReturnRows(rows)

	ch := make(chan prometheus.Metric)
	go func() {
		if err = ScrapeCfyInstances(db, ch); err != nil {
			t.Errorf("error calling function on test: %s", err)
		}
		close(ch)
	}()

	counterExpected := []MetricResult{
		{labels: labelMap{"plan_guid": "11d0aa36-dcec-4021-85f5-ea4d9a5c8342", "db_name": "cf_908c8fc1_5103_42df_a596_32b25950482a"}, value: 1048576000, metricType: dto.MetricType_UNTYPED},
		{labels: labelMap{"plan_guid": "ab08f1bc-e6fc-4b56-a767-ee0fea6e3f20", "db_name": "cf_af1ecf20_2392_4c62_b793_86426e6b897c"}, value: 104857600, metricType: dto.MetricType_UNTYPED},
		{labels: labelMap{"plan_guid": "ab08f1bc-e6fc-4b56-a767-ee0fea6e3f20", "db_name": "cf_6588904f_0372_4b62_8e62_821c966d95dd"}, value: 262144000, metricType: dto.MetricType_UNTYPED},
	}
	convey.Convey("Metrics comparison", t, func() {
		for _, expect := range counterExpected {
			got := readMetric(<-ch)
			convey.So(got, convey.ShouldResemble, expect)
		}
	})

	// Ensure all SQL queries were executed
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expections: %s", err)
	}
}
