//
// Copyright (c) 2014 The foocsim Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package utils

import (
	"fmt"
	"time"
)

type TimeDuration struct {
	duration int64
	counter  int64
}

func (d *TimeDuration) Add(delta time.Duration) {
	d.duration += delta.Nanoseconds()
	d.counter++
}

func (d *TimeDuration) MeanTimeUsecs() float64 {
	if d.counter == 0 {
		return 0.0
	}
	return (float64(d.duration) / float64(d.counter)) / 1000.0
}

func (d *TimeDuration) DeltaMeanTimeUsecs(prev *TimeDuration) float64 {
	delta := TimeDuration{}
	delta.duration = d.duration - prev.duration
	delta.counter = d.counter - prev.counter
	return delta.MeanTimeUsecs()
}

func (d *TimeDuration) Copy() *TimeDuration {
	tdcopy := &TimeDuration{}
	*tdcopy = *d
	return tdcopy
}

func (d *TimeDuration) String() string {
	return fmt.Sprintf("duration = %v\n"+
		"counter = %v\n",
		d.duration,
		d.counter)
}
