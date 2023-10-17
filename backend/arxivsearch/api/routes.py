import asyncio
import numpy as np

from redis.commands.search.query import Query
from redis.commands.search.document import Document
from redis.commands.search.result import Result

from redisvl.index import AsyncSearchIndex
from redisvl.query import VectorQuery, FilterQuery
from redisvl.query.filter import Tag, FilterExpression
from fastapi import APIRouter

from arxivsearch import config
from arxivsearch.embeddings import Embeddings
from arxivsearch.schema import (
    PaperSimilarityRequest,
    UserTextSimilarityRequest
)

from typing import List, Dict, Any, Union

paper_router = r = APIRouter()
print("Loading embeddings providers", flush=True)
embeddings = Embeddings()
paper_vector_field_name = "vector"


def process_paper(paper: Union[Document, Dict[str, Any]]) -> Dict[str, Any]:
    """
    Process paper data and calculate similarity score.

    Args:
        paper: Input paper data.

    Returns:
        dict: Processed paper data with similarity score.
    """
    if not isinstance(paper, dict):
        paper = paper.__dict__
    if 'vector_distance' in paper:
        paper['similarity_score'] = 1 - float(paper['vector_distance'])
    return paper


def build_filter_expression(years: List[int], categories: List[str]) -> FilterExpression:
    """
    Construct a filter expression based on the provided years and categories.

    Args:
        years (list): A list of years (integers or strings) to be included in the filter
                      expression. An empty list means there's no filter applied based on years.
        categories (list): A list of category strings to be included in the filter
                           expression. An empty list means there's no filter applied based
                           on categories.

    Returns:
        FilterExpression: A FilterExpression object representing the combined filter for both years and categories.
    """
    # Build filters
    year_filter = Tag("year") == [str(year) for year in years] if years else None
    category_filter = Tag("categories") == [str(category) for category in categories] if categories else None
    # Parse and create filter expression
    if not year_filter and not category_filter:
        return FilterExpression("*")
    if year_filter and category_filter:
        return year_filter & category_filter
    return year_filter or category_filter


def prepare_response(total: int, results: Union[List[Dict[str, Any]], Result]) -> Dict[str, Any]:
    """
    Extract and process papers from search results.

    This function extracts papers from the provided search results, processes each paper,
    and returns a dictionary containing the total count and a list of processed papers.

    Args:
        total (int): The hypothetical count of papers present in the db that match the filters.
        results (list): The iterable containing raw paper data.

    Returns:
        dict: A dictionary with 'total' count and a list of 'papers', where each paper is a processed dict.
    """
    # extract papers from VSS results
    if not isinstance(results, list):
        results = results.docs
    return {
        'total': total,
        'papers': [process_paper(paper) for paper in results]
    }


def create_count_query(filter_expression: FilterExpression) -> Query:
    """
    Create a "count" query where simply want to know how many records
    match a particular filter expression

    Args:
        filter_expression (FilterExpression): The filter expression for the query.

    Returns:
        Query: The Redis query object.
    """
    return (
        Query(str(filter_expression))
        .no_content()
        .dialect(2)
    )


@r.get("/", response_model=Dict)
async def get_papers(
    limit: int = 20,
    skip: int = 0,
    years: str = "",
    categories: str = ""
):
    """Fetch and return papers with optional filtering by years and categories.

    Args:
        limit (int, optional): Maximum number of papers to return. Defaults to 20.
        skip (int, optional): Number of papers to skip for pagination. Defaults to 0.
        years (str, optional): Comma-separated string of years to filter papers. Defaults to "".
        categories (str, optional): Comma-separated string of categories to filter papers. Defaults to "".

    Returns:
        dict: Dictionary containing total count and list of papers.
    """
    # Connect to index
    index_name = config.DEFAULT_PROVIDER
    index = await AsyncSearchIndex.from_existing(
        name=index_name,
        url=config.REDIS_URL
    )
    # Build query
    filter_expression = build_filter_expression(
        [year for year in years.split(",") if year],
        [cat for cat in categories.split(",") if cat]
    )
    filter_query = FilterQuery(return_fields=[], filter_expression=filter_expression)
    # Execute search
    result_papers = await index.search(
        filter_query.query.paging(skip, limit)
    )
    return prepare_response(result_papers.total, result_papers)


@r.post("/vectorsearch/paper", response_model=Dict)
async def find_papers_by_paper(similarity_request: PaperSimilarityRequest):
    """Find and return papers similar to a given paper based on vector similarity.

    Args:
        similarity_request (SimilarityRequest): Similarity request object containing paper_id, provider,
                                                number_of_results, years, and categories for filtering.

    Returns:
        dict: Dictionary containing total count and list of similar papers.
    """
    # Connect to index
    index_name = similarity_request.provider
    index = await AsyncSearchIndex.from_existing(
        name=index_name,
        url=config.REDIS_URL
    )
    # Fetch paper key and the vector from the HASH, cast to numpy array
    paper_key = index._get_key({"paper_id": similarity_request.paper_id}, "paper_id")
    # TODO - improve key parsing in redisvl
    #paper_key = index.key(similarity_request.paper_id)
    paper_vector = np.frombuffer(
        await index.client.hget(paper_key, paper_vector_field_name),
        dtype=np.float32
    )
    # Build filter expression
    filter_expression = build_filter_expression(
        similarity_request.years,
        similarity_request.categories
    )
    # Create queries
    paper_similarity_query = VectorQuery(
        vector=paper_vector,
        vector_field_name=paper_vector_field_name,
        num_results=similarity_request.number_of_results,
        return_fields=["paper_id", "authors", "categories", "year", "title", "vector_distance"],
        filter_expression=filter_expression
    )
    count_query = create_count_query(filter_expression)
    # Execute search
    count, result_papers = await asyncio.gather(
        index.search(count_query),
        index.query(paper_similarity_query)
    )
    # Get Paper records of those results
    return prepare_response(count.total, result_papers)


@r.post("/vectorsearch/text", response_model=Dict)
async def find_papers_by_text(similarity_request: UserTextSimilarityRequest):
    """Find and return papers similar to user-provided text based on vector similarity.

    Args:
        similarity_request (UserTextSimilarityRequest): Similarity request object containing user_text, provider,
                                                        number_of_results, years, and categories for filtering.

    Returns:
        dict: Dictionary containing total count and list of similar papers.
    """
    # Attach to index
    index_name = similarity_request.provider
    index = await AsyncSearchIndex.from_existing(
        name=index_name,
        url=config.REDIS_URL
    )
    # Build filter expression
    filter_expression = build_filter_expression(
        similarity_request.years,
        similarity_request.categories
    )
    # Check available paper count and create vector from user text
    count_query = create_count_query(filter_expression)
    query_vector, count = await asyncio.gather(
        embeddings.get(
            provider=index_name,
            text=similarity_request.user_text
        ),
        index.search(count_query)
    )
    # Assemble vector query
    paper_similarity_query = VectorQuery(
        vector=query_vector,
        vector_field_name=paper_vector_field_name,
        num_results=similarity_request.number_of_results,
        return_fields=["paper_id", "authors", "categories", "year", "title", "vector_distance"],
        filter_expression=filter_expression
    )
    # Perform Vector Search
    result_papers = await index.query(paper_similarity_query)
    # Get Paper records of those results
    return prepare_response(count.total, result_papers)