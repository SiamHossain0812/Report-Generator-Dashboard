import os
import re
import pandas as pd
from django.conf import settings
from django.shortcuts import render
from django.core.files.storage import FileSystemStorage

def gap_report(request):
    download_ready = False
    result_filename = None

    if request.method == 'POST' and request.FILES.get('file'):
        uploaded_file = request.FILES['file']
        fs = FileSystemStorage()

        # Step 0: Sanitize filename
        safe_filename = re.sub(r'[^\w\-_\.]', '_', uploaded_file.name)
        filename = fs.save(safe_filename, uploaded_file)
        file_path = fs.path(filename)

        try:
            # Step 1: Load CSV file
            df = pd.read_csv(file_path)

            # Step 2: Convert 'Time' column to datetime with day-first format
            df['Time'] = pd.to_datetime(df['Time'], dayfirst=True, errors='coerce')
            df.dropna(subset=['Time'], inplace=True)

            # ✅ Step 3: Keep only 15-minute interval timestamps
            df = df[df['Time'].dt.minute % 15 == 0]
            df.reset_index(drop=True, inplace=True)

            # Step 4: Replace missing indicators
            df.replace([' ', '', '-', 'NA', 'N/A'], pd.NA, inplace=True)

            # Step 5: Format time column for output
            time_col = df['Time'].dt.strftime('%d/%m/%Y %H:%M:%S').tolist()

            # Step 6: Detect missing data and gaps
            missing_values = {}
            missing_times_lists = {}

            for col in df.columns[1:]:
                series = df[col]
                missing_mask = series.isna()
                missing_values[col] = missing_mask.sum()

                times = []
                gap_start = None

                for idx, is_missing in enumerate(missing_mask):
                    if is_missing and gap_start is None:
                        gap_start = time_col[idx]
                    elif not is_missing and gap_start is not None:
                        gap_end = time_col[idx - 1]
                        if gap_start == gap_end:
                            times.append(gap_start)
                        else:
                            times.append(f"{gap_start} - {gap_end}")
                        gap_start = None

                if gap_start is not None:
                    gap_end = time_col[-1]
                    if gap_start == gap_end:
                        times.append(gap_start)
                    else:
                        times.append(f"{gap_start} - {gap_end}")

                if not times:
                    times = ['No Missing Data']

                missing_times_lists[col] = times

            # Step 7: Create output DataFrame
            columns = list(missing_values.keys())
            final_data = []
            final_data.append(['Missing_Values'] + [missing_values[col] for col in columns])

            max_len = max(len(missing_times_lists[col]) for col in columns)
            for i in range(max_len):
                row_label = 'Missing_Times' if i == 0 else ''
                row = [row_label]
                for col in columns:
                    times = missing_times_lists[col]
                    row.append(times[i] if i < len(times) else '')
                final_data.append(row)

            final_df = pd.DataFrame(final_data, columns=['Station'] + columns)

            # Step 8: Save output file
            result_filename = filename.replace('.csv', '_clean_missing_summary.csv')
            result_path = os.path.join(settings.MEDIA_ROOT, result_filename)
            final_df.to_csv(result_path, index=False)

            download_ready = True

        except Exception as e:
            print("❌ Error during processing:", e)
            download_ready = False

    return render(request, 'gap_report_generator.html', {
        'download_ready': download_ready,
        'result_filename': result_filename,
        'MEDIA_URL': settings.MEDIA_URL
    })
