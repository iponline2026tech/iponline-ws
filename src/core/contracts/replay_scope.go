package contracts

// ReplayScope limits event recovery to the connection's authorized scope.
type ReplayScope struct {
	CompanyID    *int64
	ActorKey     string
	IsSuperadmin bool
}
