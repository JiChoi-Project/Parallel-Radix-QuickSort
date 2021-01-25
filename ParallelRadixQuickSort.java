package IntSortingMethods;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class ParallelRadixQuickSort extends Sort {
    private int N_THREADS = Runtime.getRuntime().availableProcessors();
    ExecutorService pool = Executors.newFixedThreadPool(N_THREADS);
    private int THRESHOLD = 2;
    private final AtomicInteger nthread = new AtomicInteger(1);

    void algorithm() {
        /* You may change any code within this method */
        int n = this.data.length;
        
        if (n < 20000) {
            Arrays.sort(this.data);
        } else if (n < 10000000) {
            pool.execute(() -> {
                    quicksort(this.data, 0, n - 1, nthread);
                    synchronized (nthread) {
                        if (nthread.getAndDecrement() == 1)
                            nthread.notify();
                    }
            });

            try {
                synchronized (nthread) {
                    nthread.wait();
                    pool.shutdown();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        } else {
            pool.execute(() -> {
                sort(this.data, 0, n, 24, nthread);
                synchronized (nthread) {
                    if (nthread.getAndDecrement() == 1)
                        nthread.notify();
                }
            });

            try {
                synchronized (nthread) {
                    nthread.wait();
                    pool.shutdown();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    int partition(int[] array, int pLeft, int pRight) {
        getMedian(array, pLeft, pRight);
        int pivotValue = array[pRight];
        int storeIndex = pLeft;
        for (int i = pLeft; i < pRight; i++) {
            if (array[i] < pivotValue) {
                swap(array, i, storeIndex);
                storeIndex++;
            }
        }
        swap(array, storeIndex, pRight);
        return storeIndex;
    }

    void swap(int[] array, int left, int right) {
        int temp = array[left];
        array[left] = array[right];
        array[right] = temp;
    }

    private void getMedian(int[] array, int left, int right){
        int center = (left+right)/2;

        if(array[left] > array[center])
            swap(array, left, center);

        if(array[left] > array[right])
            swap(array, left, right);

        if(array[center] > array[right])
            swap(array, center, right);

        swap(array, center, right);
    }

    void quicksort(int[] array, int pLeft, int pRight, AtomicInteger nthread) {
        if (pLeft < pRight) {
            int storeIndex = partition(array, pLeft, pRight);
            int len = pRight - pLeft;
            // insertionSort
            if (len < 27) {
                for (int i = pLeft + 1; i <= pRight; i++) {
                    for (int j = i; j > pLeft && array[j] < array[j-1]; j--) {
                        swap(array, j, j - 1);
                    }
                }
                return;
            }

            if (nthread.get() >= THRESHOLD * N_THREADS || len < 300) {
                quicksort(array, pLeft, storeIndex - 1, nthread);
                quicksort(array, storeIndex + 1, pRight, nthread);
            } else {
                nthread.getAndAdd(2);
                pool.execute(() -> {
                        quicksort(array, pLeft, storeIndex - 1, nthread);
                        synchronized (nthread) {
                            if (nthread.getAndDecrement() == 1) 
                                nthread.notify();
                        }
                });

                pool.execute(() -> {
                        quicksort(array, storeIndex + 1, pRight, nthread);
                        synchronized (nthread) {
                            if (nthread.getAndDecrement() == 1) 
                                nthread.notify();
                        }
                });
            }
        }
    }


    public void sort(int[] array, int start, int end, int shift, AtomicInteger nthread) {
        int[] histogram = new int[256];
        int[] index = new int[256];

        for (int x = start; x < end; ++x) {
            ++histogram[(array[x] >> shift) & 0xFF];
        }

        histogram[0] += start;
        index[0] = start;

        if (shift == 24) {
            int neg;
            int pos;
            for (int x=128; x < 256; ++x) {
                pos = histogram[x-128];
                neg = histogram[x];
                histogram[x] = pos;
                histogram[x-128] = neg;
            }
        }

        for (int x=1; x<256; ++x) {
            index[x] = histogram[x-1];
            histogram[x] += histogram[x-1];
        }

        if (shift == 24) { // only run once
            for (int x=0; x<256; ++x) {
                while (index[x] != histogram[x]) {
                    int value = array[index[x]];
                    int y = (value >> shift) & 0xFF;
                        if (y < 128) {
                            y += 128;
                        } else {
                            y -= 128;
                        }
                    while (x != y) {
                        int temp = array[index[y]];
                        array[index[y]++] = value;
                        value = temp;
                        y = (value >> shift) & 0xFF;
                            if (y < 128) {
                                y += 128;
                            } else {
                                y -= 128;
                            }
                        }
                    array[index[x]++] = value;
                }
            }
        } else {
            for (int x=0; x<256; ++x) {
                while (index[x] != histogram[x]) {
                    int value = array[index[x]];
                    int y = (value >> shift) & 0xFF;
                    while (x != y) {
                        int temp = array[index[y]];
                        array[index[y]++] = value;
                        value = temp;
                        y = (value >> shift) & 0xFF;
                    }
                    array[index[x]++] = value;
                }
            }
        }

        if (shift > 0) {
            shift -= 8;
            for (int x=0; x<256; ++x) {
                int size = x > 0 ? index[x] - index[x-1] : index[0] - start;
                if (size > 64) {
                    if (nthread.get() >= THRESHOLD * N_THREADS) {
                        sort(array, index[x] - size, index[x], shift, nthread);
                    } else {
                       int k = x;
                       int s = shift;
                       nthread.getAndAdd(1);
                       pool.execute(() -> {
                           sort(array, index[k] - size, index[k], s, nthread);
                           synchronized (nthread) {
                               if (nthread.getAndDecrement() == 1)
                                   nthread.notify();
                           }
                       });
                    }
                } else if (size > 1) {
                    insertionSort(array, index[x] - size, index[x]);
                }
            }
        }
    }

    private void insertionSort(int array[], int start, int end) {
        for (int x=start; x<end; ++x) {
            for (int y=x; y>start && array[y-1]>array[y]; y--) {
                int temp = array[y];
                array[y] = array[y-1];
                array[y-1] = temp;
            }
        }
    }

    public String getAuthor() {
    	/* You MUST change the following line of code if you want credit.*/
        return "sap716 and jic518";
    }
}
