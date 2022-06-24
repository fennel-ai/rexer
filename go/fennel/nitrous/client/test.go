package client

// func testClient(t *testing.T, tier tier.Tier, c nitrous.Client) {
// 	keys := []string{"foo", "bar", "baz"}
// 	indices := [][]string{
// 		{"foo1", "bar1"},
// 		{"foo2", "bar2"},
// 		{"foo3", "bar3"},
// 	}
// 	vals := [][]string{
// 		{"val11", "val12"},
// 		{"val21", "val22"},
// 		{"val31", "val32"},
// 	}
// 	ctx := context.Background()
// 	getreqs := make([]nitrous.GetReq, 3)
// 	for i := range keys {
// 		getreqs[i] = nitrous.GetReq{
// 			TierID:  tier.ID,
// 			Base:    keys[i],
// 			Indices: indices[i],
// 		}
// 	}
// 	// initially nothing
// 	resps, err := c.GetMany(ctx, getreqs)
// 	assert.NoError(t, err)
// 	assert.Len(t, resps, 3)
// 	for i := range resps {
// 		assert.Equal(t, tier.ID, resps[i].TierID)
// 		assert.Equal(t, keys[i], resps[i].Base)
// 		assert.Len(t, resps[i].Data, 0)
// 	}

// 	// set some values
// 	setreqs := make([]nitrous.SetReq, 3)
// 	for i := range keys {
// 		data := make(map[string]string)
// 		for j := range indices[i] {
// 			data[indices[i][j]] = vals[i][j]
// 		}
// 		setreqs[i] = nitrous.SetReq{
// 			TierID: tier.ID,
// 			Base:   keys[i],
// 			Data:   data,
// 		}
// 	}
// 	assert.NoError(t, c.SetMany(ctx, setreqs))
// 	// wait for the set to propagate
// 	assert.NoError(t, catchup(ctx, time.Second*5, c))
// 	// get the values
// 	resps, err = c.GetMany(ctx, getreqs)
// 	assert.NoError(t, err)
// 	assert.Len(t, resps, 3)
// 	for i := range resps {
// 		assert.Equal(t, tier.ID, resps[i].TierID)
// 		assert.Equal(t, keys[i], resps[i].Base)
// 		assert.Len(t, resps[i].Data, len(vals[i]))
// 		for j, index := range indices[i] {
// 			assert.Equal(t, vals[i][j], resps[i].Data[index])
// 		}
// 	}

// 	// delete the values
// 	delreqs := make([]nitrous.DelReq, 3)
// 	for i := range keys {
// 		delreqs[i] = nitrous.DelReq{
// 			TierID:  tier.ID,
// 			Base:    keys[i],
// 			Indices: indices[i],
// 		}
// 	}
// 	assert.NoError(t, c.DelMany(ctx, delreqs))
// 	// wait for the del to propagate
// 	assert.NoError(t, catchup(ctx, time.Second*5, c))
// 	// get the values
// 	resps, err = c.GetMany(ctx, getreqs)
// 	assert.NoError(t, err)
// 	assert.Len(t, resps, 3)
// 	for i := range resps {
// 		assert.Equal(t, tier.ID, resps[i].TierID)
// 		assert.Equal(t, keys[i], resps[i].Base)
// 		assert.Len(t, resps[i].Data, 0)
// 	}
// }

// func catchup(ctx context.Context, timeout time.Duration, c nitrous.Client) error {
// 	for {
// 		select {
// 		case <-time.After(timeout):
// 			return fmt.Errorf("timed out waiting for catchup")
// 		default:
// 			lag, err := c.Lag(ctx)
// 			if err != nil {
// 				return err
// 			}
// 			if lag == 0 {
// 				return nil
// 			}
// 			time.Sleep(time.Millisecond * 100)
// 		}
// 	}
// }
