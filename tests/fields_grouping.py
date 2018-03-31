from typing import List, Any

class FieldsGrouping(object):

    def __init__(self, downstream_tasks: List[int]) -> None:

        self.downstream_tasks = downstream_tasks
        self.num_downstream = len(downstream_tasks)

    def choose_tasks(self, keys: List[Any]):

        task_index: int = 0
        prime_num: int = 633910111

        for key in keys:
            key_hash = hash(key)

            #print(f"Hashing key: {key} to {key_hash}")

            # Mod with large prime number to avoid collisions
            prime_hash = key_hash % prime_num

            #print(f"Moding with prime to {prime_hash}")

            task_index += prime_hash

        # Mod with the number of downstream keys
        task_index = task_index % self.num_downstream

        #print(f"Returning index {task_index} : "
        #      f"{self.downstream_tasks[task_index]}")

        return self.downstream_tasks[task_index]


if __name__ == "__main__":

    downstream: List[int] = [16, 17, 18, 19]
    field_grouping: FieldsGrouping = FieldsGrouping(downstream)
