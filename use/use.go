package use

import (
	"github.com/omcrgnt/res/unique"
	"github.com/omcrgnt/runner"
)

func init() {
	unique.MustAddFixed(&runner.Runner{})
}
