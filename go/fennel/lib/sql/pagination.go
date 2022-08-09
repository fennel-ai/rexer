package sql

type Pagination struct {
	Page uint `json:"page" form:"page"` // page number, start with 1
	Per  uint `json:"per" form:"per"`   // number of items per page
}

func NewPagination() Pagination {
	return Pagination{
		Page: 1,
		Per:  10,
	}
}
