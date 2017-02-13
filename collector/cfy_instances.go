// Scrape `contents of mysql_broker.service_instances`.

package collector

import (
	"fmt"
	"strings"
	"database/sql"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	// Subsystem.
	cfyInstances 		= "cfy_instances"
	cfyInstancesDb 		= "mysql_broker"
	cfyInstancesTable 	= "service_instances"
)

var cfyInstancesQueries = [2]string{
	"SHOW TABLE IN `%s` where name='%s'",
	"select plan_guid, db_name, `max_storage_mb`*1024*1024 as max_storage_mb from `%s`.`%s`",
}

func ScrapeCfyInstances(db *sql.DB, ch chan<- prometheus.Metric) error {
	var (
		cfyInstancesRows 	*sql.Rows
		cfyInstancesCols	[]string
		err             	error
	)

	for _, query := range cfyInstancesQueries {

		qry := fmt.Sprintf(query, cfyInstancesDb, cfyInstancesTable);

		if cfyInstancesRows, err = db.Query(qry); err != nil  {
			break
		}
	}

	if err != nil {
		return err
	}

	defer cfyInstancesRows.Close()

	if cfyInstancesCols, err = cfyInstancesRows.Columns(); err != nil {
		return err
	}

	for cfyInstancesRows.Next() {
		// As the number of columns varies with mysqld versions,
		// and sql.Scan requires []interface{}, we need to create a
		// slice of pointers to the elements of slaveData.
		scanArgs := make([]interface{}, len(cfyInstancesCols))
		for i := range scanArgs {
			scanArgs[i] = &sql.RawBytes{}
		}

		if err := cfyInstancesRows.Scan(scanArgs...); err != nil {
			return err
		}

		plan 	:= columnValue(scanArgs, cfyInstancesCols, "plan_guid")
		dbname 	:= columnValue(scanArgs, cfyInstancesCols, "db_name") // MariaDB

		for i, col := range cfyInstancesCols {
			if value, ok := parseStatus(*scanArgs[i].(*sql.RawBytes)); ok { // Silently skip unparsable values.
				ch <- prometheus.MustNewConstMetric(
					prometheus.NewDesc(
						prometheus.BuildFQName(namespace, cfyInstances, strings.ToLower(col)),
						"Generic metric from MySQL Broker Service instances.",
						[]string{"plan_guid", "db_name"},
						nil,
					),
					prometheus.UntypedValue,
					value,
					plan, dbname,
				)
			}
		}
	}
	return nil
}
