// Copyright (c) 2019 Palantir Technologies. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package utils

// StringSet is a non-thread safe set of unique strings
type StringSet struct {
	elements map[string]bool
}

// NewStringSet constructs and returns a StringSet
func NewStringSet(size int) *StringSet {
	return &StringSet{
		elements: make(map[string]bool, size),
	}
}

// Add adds the string e to the StringSet if it is not already there
func (s *StringSet) Add(e string) {
	s.elements[e] = true
}

// Remove removes the string e from the StringSet if it is there. It is a no-op otherwise
func (s *StringSet) Remove(e string) {
	delete(s.elements, e)
}

// Contains returns true if the string e is in the StringSet, false otherwise
func (s *StringSet) Contains(e string) bool {
	return s.elements[e]
}

// Size returns the number of elements in the StringSet
func (s *StringSet) Size() int {
	return len(s.elements)
}

// ToSlice returns all the elements of StringSet as a slice of strings
func (s *StringSet) ToSlice() []string {
	result := make([]string, len(s.elements))
	for e := range s.elements {
		result = append(result, e)
	}
	return result
}
