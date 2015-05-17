# Minimalistic assertion library

## Installation
go get -u github.com/hooklift/assert

### Usage

```go
package blah

import (
	"testing"

	"github.com/hooklift/assert"
)

func TestSave(t *testing.T) {
	u := &Account{
		repo:     &RepoMock{},
		Email:    "camilo@hooklift.io",
		Password: "mypassword",
		Name:     "Camilo Aguilar",
	}

	err := u.Save()
	assert.Ok(t, err)
	assert.Cond(t, u.ID != "", "User ID should not be empty: %v", u)

	u2, err := Get(u.ID)
	assert.Ok(t, err)
	assert.Equals(t, u.ID, u2.ID)
}
```