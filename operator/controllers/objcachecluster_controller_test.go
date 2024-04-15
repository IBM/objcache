/*
 * Copyright 2023- IBM Inc. All rights reserved
 * SPDX-License-Identifier: Apache-2.0
 */

package controllers

import (
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"
)

func TestQuantity(t *testing.T) {
	cap, _ := resource.ParseQuantity("40Gi")
	t.Errorf("%v", cap)
	cap2 := cap.ScaledValue(resource.Scale(3))
	cap3 := cap.Value()
	cacheCapacity := int64(float64(cap.Value()) * 80 / 100)
	cacheCapacityInQuantity := resource.NewQuantity(cap.Value(), cap.Format)
	cacheCapacityInQuantity.Set(cacheCapacity)
	t.Errorf("cap2: %v, cap3: %v, cacheCapacity: %v, cacheCapacityInQuantity: %v", cap2, cap3, cacheCapacity, cacheCapacityInQuantity.String())
}
