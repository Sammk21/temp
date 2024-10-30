import scrapy
from w3lib.html import remove_tags_with_content
from scrapy import Request
import logging
from collections import defaultdict
from datetime import datetime
import json
import time


class CollegesSpider(scrapy.Spider):
    name = "colleges_spider"
    allowed_domains = ["collegedekho.com"]
    start_urls = ["https://www.collegedekho.com/medical/colleges-in-india/"]

    custom_settings = {
        "CONCURRENT_REQUESTS_PER_DOMAIN": 2,  # Reduced concurrent requests
        "ROBOTSTXT_OBEY": False,
        "CONCURRENT_REQUESTS": 1,
        "LOG_LEVEL": "INFO",
        "RETRY_ENABLED": True,
        "RETRY_TIMES": 5,  # Increased retry times
        "RETRY_HTTP_CODES": [500, 502, 503, 504, 408, 429, 403],
        "DOWNLOAD_DELAY": 4,  # Increased delay
        "DOWNLOAD_TIMEOUT": 180,  # Increased timeout
        "COOKIES_ENABLED": True,
        # Add middleware to rotate User-Agents
        "DOWNLOADER_MIDDLEWARES": {
            "scrapy.downloadermiddlewares.useragent.UserAgentMiddleware": None,
            "scrapy_user_agents.middlewares.RandomUserAgentMiddleware": 400,
        },
    }

    def __init__(self, *args, **kwargs):
        super(CollegesSpider, self).__init__(*args, **kwargs)
        self.total_blocks = 0
        self.total_pages = 0
        self.blocks_per_page = defaultdict(int)
        self.failed_pages = set()
        self.failed_colleges = set()
        self.successful_colleges = set()
        self.college_attempts = defaultdict(int)
        self.page_attempts = defaultdict(int)
        self.college_data = {}
        self.college_tabs_pending = {}

        # Progress tracking
        self.progress_file = f"scraping_progress_{int(time.time())}.json"
        self.load_progress()

        # Statistics
        self.stats = {
            "start_time": datetime.now().isoformat(),
            "pages_processed": 0,
            "colleges_found": 0,
            "colleges_scraped": 0,
            "failed_attempts": 0,
        }

    total_blocks = 0
    total_pages = 0

    def start_requests(self):
        # Set to scrape all 236 pages
        max_pages = 81
        for page in range(1, max_pages + 1):
            url = f"{self.start_urls[0]}?page={page}"
            yield scrapy.Request(
                url,
                callback=self.parse,
                meta={
                    "page": page,
                    "dont_retry": False,
                    "handle_httpstatus_list": [400, 404, 500, 503],
                },
                errback=self.handle_error,
                dont_filter=True,  # Allow duplicate requests if needed
            )

    def parse(self, response):
        page_number = response.meta["page"]

        if not self.is_valid_page(response):
            return self.handle_invalid_page(response)

        # Get all college blocks with multiple selectors for redundancy
        college_blocks = set()
        selectors = [
            "div.collegeCardBox.col-md-12",
        ]

        for selector in selectors:
            blocks = response.css(selector)
            college_blocks.update(blocks)

        college_blocks = list(college_blocks)
        block_count = len(college_blocks)

        self.logger.info(f"Page {page_number}: Found {block_count} college blocks")
        self.stats["colleges_found"] += block_count

        if block_count < 10:  # Expected minimum blocks per page
            if self.page_attempts[page_number] < 3:
                return self.retry_page(response)
            else:
                self.failed_pages.add(page_number)
                self.logger.error(
                    f"Page {page_number} failed after 3 attempts - too few blocks"
                )

        # Process each block with detailed logging
        for index, college in enumerate(college_blocks, 1):
            college_data = self.extract_college_data(college)
            college_link = college.css("div.titleSection h3 a::attr(href)").get()

            if college_link and college_link not in self.progress["completed_colleges"]:
                yield response.follow(
                    college_link,
                    self.parse_college_page,
                    meta={
                        "college_data": college_data,
                        "page_number": page_number,
                        "block_number": index,
                        "college_link": college_link,
                        "dont_retry": False,
                        "download_timeout": 180,
                    },
                    errback=self.handle_error,
                    dont_filter=True,
                    priority=5,  # Lower priority than main pages
                )
            elif college_link in self.progress["completed_colleges"]:
                self.logger.info(f"Skipping already processed college: {college_link}")

    def extract_college_data(self, college):
        """Extract all college data from a block with error handling"""
        try:
            # Your existing data extraction code here
            title = college.css("div.titleSection h3 a::text").get()
            title = title.strip() if title else "Unknown Title"

            # Location information
            location_info = college.css(
                "div.collegeinfo ul.info li:nth-child(2)::text"
            ).get()
            city, state = None, None
            if location_info:
                if ", " in location_info:
                    city, state = location_info.split(", ")
                else:
                    city = location_info

            # Rest of your extraction code...

            ownership = None
            ownership_li = college.css("div.collegeinfo ul.info li")
            for li in ownership_li:
                img_src = li.css("img::attr(src)").get()
                if img_src and "flag.29bda52542d4.svg" in img_src:
                    ownership_texts = li.css("::text").getall()
                    ownership = ownership_texts[-1].strip() if ownership_texts else None
                    break

            # Fetch ranking and rank publisher
            ranking = college.css("div.collegeinfo ul.info li b span::text").get()
            ranking = ranking.replace("#", "").strip() if ranking else None
            rank_publisher = college.css("div.collegeinfo ul.info li b::text").re_first(
                r"\s*(\w+)"
            )

            # Fetch fees
            fees = college.css(
                'div.fessSection li img[src*="rupeeListing"] + p::text'
            ).get()
            fees = fees.strip() if fees else None

            # Fetch accreditation
            accreditation = college.css(
                'div.fessSection li img[src*="batch"] + p::text'
            ).get()
            accreditation = accreditation.strip() if accreditation else None

            # Fetch average package
            avg_package = college.css(
                'div.fessSection li img[src*="symbol"] + p::text'
            ).get()
            avg_package = avg_package.strip() if avg_package else None

            # Fetch exam information
            exams = []
            exam_li = college.css(
                'div.fessSection li img[src*="exam.57dec076328a.svg"]'
            )
            if exam_li:
                main_exam = exam_li.xpath(
                    "following-sibling::p/text()[normalize-space()]"
                ).get()
                main_exam = main_exam.strip() if main_exam else None
                if main_exam:
                    exams.append(main_exam)

                tooltip_exams_text = exam_li.xpath(
                    'following-sibling::div[@class="tooltip"]//span[@class="hover"]/text()'
                ).get()
                if tooltip_exams_text:
                    tooltip_exams = [
                        exam.strip()
                        for exam in tooltip_exams_text.split(",")
                        if exam.strip()
                    ]
                    exams.extend(tooltip_exams)

            exams = list(set(exams))  # Remove duplicates if any

            # Fetch description
            description = college.css(".content .ReadMore::text").get()
            description = description.strip() if description else None

            # Prepare the college data dictionary
            return {
                "title": title,
                "city": city,
                "state": state,
                "ownership": ownership,
                "ranking": ranking,
                "rank_publisher": rank_publisher,
                "fees": fees,
                "accreditation": accreditation,
                "avg_package": avg_package,
                "exams": exams,
                "description": description,
            }

        except Exception as e:
            self.logger.error(f"Error extracting data: {str(e)}")
            return {"title": "Error in extraction", "error": str(e)}

    def parse_college_page(self, response):
        college_data = response.meta["college_data"]

        # Extract overview and other sections
        overviewTab, facilities = self.extract_overview_tab(response)
        college_data["overviewTab"] = overviewTab
        college_data["facilities"] = facilities
        college_data["highlights"] = self.extract_highlights(response)
        college_data["courses"] = self.extract_courses(response)
        college_data["faqs"] = self.extract_faqs(response)
        college_data["tabs"] = {}  # Initialize tabs dictionary

        # Extract sub-navigation tabs
        nav_tabs = response.css(".container.mobileContainerNone ul li a")
        tabs_to_fetch = [
            tab
            for tab in nav_tabs
            if tab.css("::text").get().strip()
            not in ["Gallery", "Reviews", "News", "QnA"]
        ]
        college_data["tabs_pending"] = len(tabs_to_fetch)  # Set up pending tabs counter

        # Process each sub-navigation tab
        for tab in tabs_to_fetch:
            tab_title = tab.css("::text").get().strip()
            tab_url = tab.css("::attr(href)").get()

            self.logger.info(f"Processing tab: {tab_title}")

            yield response.follow(
                tab_url,
                self.parse_tab_content,
                meta={
                    "college_data": college_data,  # Direct reference, no copy
                    "tab_title": tab_title,
                },
                dont_filter=True,
            )

        # If no tabs to fetch, yield immediately
        if college_data["tabs_pending"] == 0:
            yield college_data

    def parse_tab_content(self, response):
        college_data = response.meta["college_data"]
        tab_title = response.meta["tab_title"]
        tab_key = f"{tab_title.replace(' ', '').lower()}Tab"

        # Initialize tab data structure
        tab_data = {"tab": tab_title, "content": []}
        self.logger.info(f"Fetching content for tab: {tab_title}")

        blocks = response.css(".block.box")
        for block in blocks:
            title = self.safe_extract(block, "h2::text")
            content_html = block.css(".collegeDetail_classRead__yd_kT").get()

            if (
                title
                and content_html
                and not any(item["title"] == title for item in tab_data["content"])
            ):
                tab_data["content"].append({"title": title, "content": content_html})

        # Add tab data to college_data tabs dictionary
        college_data["tabs"][tab_key] = tab_data

        # Update pending tabs counter and yield if all tabs are complete
        college_data["tabs_pending"] -= 1
        if college_data["tabs_pending"] == 0:
            college_data.pop("tabs_pending")  # Clean up
            yield college_data

    def load_progress(self):
        """Load previous progress if exists"""
        try:
            with open(self.progress_file, "r") as f:
                self.progress = json.load(f)
        except FileNotFoundError:
            self.progress = {
                "completed_pages": set(),
                "completed_colleges": set(),
                "failed_colleges": set(),
            }

    def save_progress(self):
        """Save current progress"""
        progress_data = {
            "completed_pages": list(self.progress["completed_pages"]),
            "completed_colleges": list(self.progress["completed_colleges"]),
            "failed_colleges": list(self.progress["failed_colleges"]),
            "stats": self.stats,
        }
        with open(self.progress_file, "w") as f:
            json.dump(progress_data, f)

    def extract_overview_tab(self, response):
        overviewTab = []
        static_blocks = response.css(".collegeDetailContainer")
        for block in static_blocks:
            title = block.css(".sectionHeadingSpace h2::text").get()
            content = block.xpath(
                './/div[contains(@class, "staticContent_staticContentBlcok__MmmkX")]'
                '/div[not(contains(@class, "staticContent_hideContent__fj6cN")) and not(contains(@class, "BannerContent_readMore__WMDLd"))]'
                '| .//div[contains(@class, "staticContent_hideContent__fj6cN") or contains(@class, "BannerContent_readMore__WMDLd")]/*'
            ).getall()

            title = title.strip() if title else None
            content = [
                remove_tags_with_content(c, which_ones=("a", "img")) for c in content
            ]
            content_html = "".join(content).strip()

            if (
                title
                and content_html
                and not any(item["title"] == title for item in overviewTab)
            ):
                overviewTab.append({"title": title, "content": content_html})

        facilities = response.css(
            ".campusFacilities_facilityCardsContainer__lnH1y .facilityCard_facilityCards__qdCoE::text"
        ).getall()
        facilities = [
            facility.strip() for facility in facilities if facility.strip()
        ]  # Clean up any extra spaces

        # Return both overviewTab and facilities as separate data points
        return overviewTab, facilities

    def extract_highlights(self, response):
        highlights = response.css(".collegeHighlightsCard_collegeHighlightBox__Efa_o")
        highlight_dict = {}
        for item in highlights:
            key = item.css(".collegeHighlightsCard_highlightName__NP6u9::text").get()
            value = item.css(".collegeHighlightsCard_highlightLabel__5B3__::text").get()
            if key and value:
                highlight_dict[key.strip()] = value.strip()
        return highlight_dict

    def extract_courses(self, response):
        courses = []
        course_blocks = response.css(".courseCard_courseCard__dfnvS")
        for course in course_blocks:
            course_data = {
                "course_title": self.safe_extract(
                    course, ".courseName_courseHeading__CudEq a::text"
                ),
                "fees": self.safe_extract(
                    course, ".courseCardDetail_detailBoldText__ukBXc::text"
                ),
                "duration": self.safe_extract(
                    course, ".courseCardDetail_courseDetailList__eCaZU div::text"
                ),
                "study_mode": self.safe_extract(
                    course,
                    ".courseCardDetail_courseDetailList__eCaZU div:nth-child(3)::text",
                ),
                "eligibility": self.safe_extract(
                    course, ".courseCardDetail_eligibilityText__H12Xm::text"
                ),
                "offered_courses": course.css(
                    ".courseCardDetail_detailBoldText__ukBXc span span::attr(title)"
                ).getall(),
            }
            course_data = {k: v for k, v in course_data.items() if v}
            if course_data.get("course_title") and not any(
                c["course_title"] == course_data["course_title"] for c in courses
            ):
                courses.append(course_data)
        return courses

    def extract_faqs(self, response):
        faqs = []
        faq_blocks = response.css(".accordion_accordionInner__J27vt")
        for faq in faq_blocks:
            question = self.safe_extract(faq, "h3::text")
            answer = self.safe_extract(faq, ".accordion_content__KQYJ_ div::text")
            if question and answer and not any(f["question"] == question for f in faqs):
                faqs.append({"question": question, "answer": answer})
        return faqs

    def extract_facilities(self, response):
        facilities_section = response.css(".collegeDetail_facilities__wrgyU")
        if facilities_section:
            facilities = facilities_section.css("ul li p::text").getall()
            return [facility.strip() for facility in facilities if facility.strip()]
        return None

    def safe_extract(self, selector, css_selector):
        extracted = selector.css(css_selector).get()
        return extracted.strip() if extracted else None

    def closed(self, reason):
        """Spider closure stats and reporting"""
        self.logger.info("\n=== Spider Completion Report ===")
        self.logger.info(f"Total pages processed: {self.total_pages}")
        self.logger.info(f"Total blocks found: {self.total_blocks}")
        self.logger.info(
            f"Average blocks per page: {self.total_blocks/self.total_pages if self.total_pages else 0:.2f}"
        )

        # Report pages with no blocks
        empty_pages = [
            page for page, count in self.blocks_per_page.items() if count == 0
        ]
        if empty_pages:
            self.logger.warning(f"Pages with no blocks: {empty_pages}")

        # Report failed pages
        if self.failed_pages:
            self.logger.error(f"Failed pages: {self.failed_pages}")

        # Save stats to file
        with open("scraping_stats.txt", "w") as f:
            f.write(f"Total pages: {self.total_pages}\n")
            f.write(f"Total blocks: {self.total_blocks}\n")
            f.write("Blocks per page:\n")
            for page, count in sorted(self.blocks_per_page.items()):
                f.write(f"Page {page}: {count} blocks\n")
            if self.failed_pages:
                f.write(f"\nFailed pages: {self.failed_pages}\n")

    def validate_blocks(self, response, block_count, page_number):
        """Validate if we got the expected number of blocks"""
        # If blocks are significantly less than expected
        if block_count < (self.expected_blocks_per_page * 0.8):  # 80% threshold
            retry_count = self.retry_pages.get(page_number, 0)
            if retry_count < 3:  # Maximum 3 retries
                self.retry_pages[page_number] = retry_count + 1
                self.logger.warning(
                    f"Page {page_number}: Found only {block_count} blocks "
                    f"(expected ~{self.expected_blocks_per_page}). Retrying..."
                )

                # Return a new request for this page
                return scrapy.Request(
                    response.url,
                    callback=self.parse,
                    meta={
                        "page": page_number,
                        "retry_count": retry_count + 1,
                        "download_delay": (retry_count + 1)
                        * 2,  # Increase delay with each retry
                    },
                    dont_filter=True,
                )
            else:
                self.logger.error(
                    f"Page {page_number}: Still only found {block_count} blocks "
                    f"after {retry_count} retries. Moving on..."
                )

        return None

    def is_valid_page(self, response):
        """Check if the page response is valid"""
        # Check for expected elements
        has_college_blocks = bool(response.css("div.collegeCardBox"))
        has_expected_structure = bool(response.css("div.container"))
        content_length = len(response.body) > 1000  # Minimum expected page size

        return has_college_blocks and has_expected_structure and content_length

    def handle_invalid_page(self, response):
        """Handle invalid page responses"""
        page_number = response.meta["page"]
        self.logger.warning(f"Invalid page detected: {page_number}")
        return self.retry_page(response)

    def retry_page(self, response):
        """Retry fetching a page with exponential backoff"""
        page_number = response.meta["page"]
        self.page_attempts[page_number] += 1
        retry_count = self.page_attempts[page_number]

        if retry_count <= 3:
            delay = 2**retry_count  # Exponential backoff
            self.logger.info(
                f"Retrying page {page_number} (attempt {retry_count}) with delay {delay}s"
            )

            return scrapy.Request(
                (
                    response.url
                    if isinstance(response, scrapy.http.Response)
                    else response.url
                ),
                callback=self.parse,
                meta={
                    "page": page_number,
                    "retry_count": retry_count,
                    "download_delay": delay,
                    "dont_retry": False,
                },
                dont_filter=True,
                priority=10,
            )

    def handle_error(self, failure):
        page = failure.request.meta.get("page")
        self.failed_pages.append(page)
        self.logger.error(f"Failed to fetch page {page}: {str(failure.value)}")

        # Retry the failed request with increased delay
        if failure.request.meta.get("retry_count", 0) < 3:
            retry_count = failure.request.meta.get("retry_count", 0) + 1
            self.logger.info(f"Retrying page {page} (attempt {retry_count})")

            new_request = failure.request.copy()
            new_request.meta["retry_count"] = retry_count
            new_request.meta["download_delay"] = (
                5 * retry_count
            )  # Increase delay with each retry

            yield new_request
