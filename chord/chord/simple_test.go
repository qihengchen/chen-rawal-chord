package chord

import (
	"testing"
	"strconv"
)

var n1, n2, n3 *Node
var err error

func TestCreateNode(t *testing.T) {
	n1, err = CreateNode(nil)
	if err != nil {
		t.Errorf("Unable to create first node, received error:%v\n", err)
	}
}

func TestCreateSecondNode(t *testing.T) {
	_, err := CreateNode(n1.RemoteSelf)
	if err != nil {
		t.Errorf("Unable to create second node, received error:%v\n", err)
	}
}

func TestCreateThirdNode(t *testing.T) {
	_, err := CreateNode(n1.RemoteSelf)
	if err != nil {
		t.Errorf("Unable to create third node, received error:%v\n", err)
	}
}

func TestPut(t *testing.T) {
	var err error
	for i := 1; i <= 50; i++ {
		i_str := strconv.Itoa(i)
		if err = Put(n1, "n1_"+i_str, i_str); err != nil {
			t.Errorf("Unable to put data, received error:%v\n", err)
		}
	}
}

func TestGet(t *testing.T) {
	var err error
	var v string
	if v, err = Get(n1, "n1_1"); err != nil {
		t.Errorf("Unable to get data %v, received error:%v\n", v, err)
	}
	if v, err = Get(n1, "n1_50"); err != nil {
		t.Errorf("Unable to get data %v, received error:%v\n", v, err)
	}
	if v, err = Get(n1, "n1_51"); err == nil {
		t.Errorf("Unable to get err for fake key %v, received error:%v\n", v, err)
	}
	if v, err = Get(n1, "n1_0"); err == nil {
		t.Errorf("Unable to get err for fake key %v, received error:%v\n", v, err)
	}
}

func TestTransferKeys(t *testing.T) {
	
}

func TestShutDown(t *testing.T) {

}

