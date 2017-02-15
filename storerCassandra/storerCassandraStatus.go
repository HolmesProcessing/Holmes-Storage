package StorerCassandra

import (
	"errors"
	"fmt"
	"time"

	"github.com/gocql/gocql"

	"github.com/HolmesProcessing/Holmes-Storage/storerGeneric"
)

/*
  To list all tables:
    SELECT keyspace_name, table_name FROM system_schema.tables;

  To list all columns in a table:
    SELECT * FROM system_schema.columns WHERE
    keyspace_name = 'holmes_status' AND
    table_name    = 'planners';

  Database setup:
    CREATE KEYSPACE holmes_status
    WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': '2'};
*/

func (s StorerCassandra) PrepareClusterStatus(c []*storerGeneric.DBConnector) (*gocql.ClusterConfig, error) {
	if len(c) < 1 {
		return nil, errors.New("Supply at least one node to connect to!")
	}

	connStrings := make([]string, len(c))
	for i, elem := range c {
		connStrings[i] = fmt.Sprintf("%s:%d", elem.IP, elem.Port)
	}

	if c[0].StatusDatabase == "" {
		return nil, errors.New("Please supply a database/keyspace to use (Config-Key: HolmesStatus)!")
	}

	cluster := gocql.NewCluster(connStrings...)
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: c[0].User,
		Password: c[0].Password,
	}
	cluster.ProtoVersion = 4
	return cluster, nil
}

func (s StorerCassandra) CreateDBStatus(c []*storerGeneric.DBConnector) error {
	var (
		cluster *gocql.ClusterConfig
		db      *gocql.Session
		query   string
		err     error
	)

	if cluster, err = s.PrepareClusterStatus(c); err != nil {
		return err
	}
	cluster.Keyspace = "system"
	cluster.Timeout = 20 * time.Second

	if db, err = cluster.CreateSession(); err != nil {
		return err
	}

	query = fmt.Sprintf(`CREATE KEYSPACE IF NOT EXISTS %s WITH replication =
    {'class': 'NetworkTopologyStrategy', 'dc1': '2'};`, c[0].StatusDatabase)

	if err := db.Query(query).Exec(); err != nil {
		return err
	}

	db.Close()
	return nil
}

func (s StorerCassandra) InitializeStatus(c []*storerGeneric.DBConnector) (storerGeneric.Storer, error) {
	var (
		cluster *gocql.ClusterConfig
		err     error
	)

	if cluster, err = s.PrepareClusterStatus(c); err != nil {
		return s, err
	}
	cluster.Keyspace = c[0].StatusDatabase
	cluster.Consistency = gocql.Quorum
	// cluster.Consistency = gocql.LocalOne
	cluster.Timeout = 20 * time.Second

	s.StatusDB, err = cluster.CreateSession()

	return s, err
}

func (s StorerCassandra) SetupStatus() error {
	tableMachines := `
    CREATE TABLE IF NOT EXISTS machines (
      machine_uuid        UUID,
      harddrives          TEXT,
      network_interfaces  TEXT,
      first_seen          TIMESTAMP,
      last_seen           TIMESTAMP,
      PRIMARY KEY ((machine_uuid))
    );
  `
	if err := s.DB.Query(tableMachines).Exec(); err != nil {
		return err
	}

	// this table is not satisfactory ...
	// tableMachinesLastSeenTs := `
	//    CREATE TABLE IF NOT EXISTS machines_lastseen_ts (
	//      machine_uuid        UUID,
	//      last_seen           TIMESTAMP,
	//      PRIMARY KEY ((last_seen))
	//    )
	//    WITH
	//      CLUSTERING ORDER BY (last_seen DESC)
	//    AND
	//      compaction = {
	//        'class': 'DateTieredCompactionStrategy',
	//        'base_time_seconds':'300'
	//      }
	//    ;
	//  `
	// if err := s.DB.Query(tableMachinesLastSeenTs).Exec(); err != nil {
	// 	return err
	// }

	tableMachinesSystemStatusTs := `
    CREATE TABLE IF NOT EXISTS machines_systemstatus_ts (
      machine_uuid  UUID,
      cpu_iowait    BIGINT, -- uint64
      cpu_idle      BIGINT, -- uint64
      cpu_busy      BIGINT, -- uint64
      cpu_total     BIGINT, -- uint64
      mem_usage     BIGINT, -- uint64
      mem_max       BIGINT, -- uint64
      swap_usage    BIGINT, -- uint64
      swap_max      BIGINT, -- uint64
      loads_1       DOUBLE,
      loads_5       DOUBLE,
      loads_15      DOUBLE,
      timestamp     TIMESTAMP,
      PRIMARY KEY ((machine_uuid), timestamp)
    )
    WITH
      CLUSTERING ORDER BY (timestamp DESC)
    AND
      compaction = {
        'class': 'DateTieredCompactionStrategy',
        'base_time_seconds':'300'
      }
    ;
  `
	if err := s.DB.Query(tableMachinesSystemStatusTs).Exec(); err != nil {
		return err
	}

	tablePlanners := `
    CREATE TABLE IF NOT EXISTS planners (
      planner_uuid  UUID,
      machine_uuid  UUID,
      name          TEXT,
      ip            TEXT,
      port          INT,
      configuration TEXT,
      first_seen    TIMESTAMP,
      last_seen     TIMESTAMP,
      PRIMARY KEY ((machine_uuid), planner_uuid)
    );
  `
	if err := s.DB.Query(tablePlanners).Exec(); err != nil {
		return err
	}

	// tablePlannersLastSeenTs := `
	//    CREATE TABLE IF NOT EXISTS planners_lastseen_ts (
	//      machine_uuid UUID,
	//      last_seen    TIMESTAMP,
	//      planner_uuid UUID,
	//      PRIMARY KEY ((machine_uuid), last_seen)
	//    )
	//    WITH
	//      CLUSTERING ORDER BY (last_seen DESC)
	//    AND
	//      compaction = {
	//        'class': 'DateTieredCompactionStrategy',
	//        'base_time_seconds':'300'
	//      }
	//    ;
	//  `
	// if err := s.DB.Query(tablePlannersLastSeenTs).Exec(); err != nil {
	// 	return err
	// }

	tablePlannersLogsTs := `
    CREATE TABLE IF NOT EXISTS planners_logs_ts (
      planner_uuid  UUID,
      timestamp     TIMESTAMP,
      message       TEXT,
      PRIMARY KEY ((planner_uuid), timestamp)
    )
    WITH
      CLUSTERING ORDER BY (timestamp DESC)
    AND
      compaction = {
        'class': 'DateTieredCompactionStrategy',
        'base_time_seconds':'300'
      }
    ;
  `
	if err := s.DB.Query(tablePlannersLogsTs).Exec(); err != nil {
		return err
	}

	tableServices := `
    CREATE TABLE IF NOT EXISTS services (
      planner_uuid  UUID,
      port          INT,
      service_uuid  UUID,
      name          TEXT,
      configuration TEXT,
      PRIMARY KEY ((planner_uuid), port)
    );
  `
	if err := s.DB.Query(tableServices).Exec(); err != nil {
		return err
	}

	tableServicesLogsTs := `
    CREATE TABLE IF NOT EXISTS services_logs_ts (
      service_uuid UUID,
      timestamp    TIMESTAMP,
      message      TEXT,
      PRIMARY KEY ((service_uuid), timestamp)
    )
    WITH
      CLUSTERING ORDER BY (timestamp DESC)
    AND
      compaction = {
        'class': 'DateTieredCompactionStrategy',
        'base_time_seconds':'300'
      }
    ;
  `
	if err := s.DB.Query(tableServicesLogsTs).Exec(); err != nil {
		return err
	}

	tableServicesTasksTs := `
    CREATE TABLE IF NOT EXISTS services_tasks_ts (
      service_uuid UUID,
      timestamp    TIMESTAMP,
      task         TEXT,
      PRIMARY KEY ((service_uuid), timestamp)
    )
    WITH
      CLUSTERING ORDER BY (timestamp DESC)
    AND
      compaction = {
        'class': 'DateTieredCompactionStrategy',
        'base_time_seconds':'300'
      }
    ;
  `
	if err := s.DB.Query(tableServicesTasksTs).Exec(); err != nil {
		return err
	}

	// parent = 00000-.....-00000 = neutral ID
	tableKvStore := `
    CREATE TABLE IF NOT EXISTS kvstore (
      parent  UUID,
      id      UUID,
      key     VARCHAR,
      path    VARCHAR,
      value   TEXT,
      PRIMARY KEY ((parent), id)
    )
    WITH
      CLUSTERING ORDER BY (id ASC)
    ;
  `
	if err := s.DB.Query(tableKvStore).Exec(); err != nil {
		return err
	}

	tableKvStoreSecondary := `
    CREATE TABLE IF NOT EXISTS kvstore_secondary (
      path    VARCHAR,
      parent  UUID,
      id      UUID,
      PRIMARY KEY ((path))
    );
  `
	if err := s.DB.Query(tableKvStoreSecondary).Exec(); err != nil {
		return err
	}

	return nil
}
