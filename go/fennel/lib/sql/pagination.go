package sql

type Pagination struct {
	Page uint32 `json:"page"` // page number, start with 1
	Per  uint32 `json:"per"`  // number of items per page
}

func NewPagination() Pagination {
	return Pagination{
		Page: 1,
		Per:  10,
	}
}
