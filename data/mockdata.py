# This function is used solely for generating fake/mock data to simulate API calls.
# It is not intended for use in production or with real blockchain data.


def generate_target_calls(
    start_id: int, end_id: int, base_url: str = "https://api.infura.com/block/"
):
    return [
        {
            "call_id": call_id,  # Unique identifier for each fake API call.
            "block_number": block_number,  # Simulated block number starting from 100.
            "endpoint": f"{base_url}{block_number}",  # Fake API endpoint using the block number.
        }
        for call_id, block_number in zip(
            range(start_id, end_id + 1),  # Generates call IDs from start_id to end_id.
            range(
                100, 100 + (end_id - start_id) + 1
            ),  # Generates block numbers starting from 100.
        )
    ]


# Generates a large list of fake target calls (in this case, 100,000 calls).
TARGET_CALLS = generate_target_calls(1, 100000)
